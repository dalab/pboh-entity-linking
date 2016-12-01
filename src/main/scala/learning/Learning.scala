package learning

import gnu.trove.map.hash.{THashMap, TObjectDoubleHashMap, TLongDoubleHashMap}
import gnu.trove.set.hash.TLongHashSet

import scala.io.Source
import index.AllIndexesBox
import eval.VerifyEDLBPForListOfWords
import scala.util.control.Breaks._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import index.MentionEntitiesFrequencyIndex
import learning.memcached.MemClient
import learning.memcached.SGDWeightMemClient
import utils.OptimizedRhosMap
import utils.OptimizedLambdasMap
import learning.memcached.SGDPiecewiseMemClient
import loopybeliefpropagation.ScorerFull
import eval.EvalOnDatasets
import scala.collection.mutable.ArrayBuffer
import loopybeliefpropagation.ScorerWeights

/* General class that loads the training instances (x_i,y_i)_{i=1..N} and
 * uses SGD to train the proper parameters by calling a specific update function
 * corresponding to a specific training method (StructSVM, piecewise training, etc).
 * 
 * To run: master node is different than worker nodes (3 nodes) and different 
 * than memcached nodes (4 nodes), run with -Xss1g
 *  $ for i in `seq -w 33 40`; do ssh root@dco-node0$i.dco.ethz.ch 'killall memcached';  done
 *  $ for i in `seq -w 33 36`; do ssh root@dco-node0$i.dco.ethz.ch 'memcached -d -u root -M -m 30000 -p 11211'; done
 *  $ MASTER=spark://dco-node040.dco.ethz.ch:7077 /mnt/cw12/Octavian/spark-1.1.1-bin-hadoop2.4/bin/spark-submit
 *   --class "EL_LBP_Spark" 
 *   --jars lib/el_lbp_clueweb_list_words_2.10-1.0.jar,lib/trove-3.1a1.jar,lib/spymemcached-2.10.3.jar lib/el-lbp.jar 
 *   learnLoopyParams /mnt/cw12/Octavian/el-lbp-marina/marinah/wikipedia/anchorsListFromEachWikiPage.txt
 *    > >(tee out.log) 2> >(tee err.log >&2)
 */

object Learning {

  // Training data in form of (pageid, page_list_mention_entity) spitted per machines.
  var splittedTrainingDataRDD : RDD[Array[(Int, Array[(String,Int, Array[Int])])]] = null
  var validationData : Array[(Int, Array[(String,Int, Array[Int])])] = null 
  var testData : Array[(Int, Array[(String,Int, Array[Int])])] = null 
  
  /*
   * For loading parameters into memcached and for testing.
   */
  val a = 0.09 
  val f = 1.0
  val g = 0.29
  val h = 0.17  
  val ss = 1.0
  val b = 1.0

  
  ////////////////// Learning phase ////////////////////////////////
  def getFreqPairsOfEnts(args : Array[String]) = {
    //////////////////// SPARK context ///////////////////////////////
    val conf = new SparkConf().setAppName("EL LBP SPARK " + System.currentTimeMillis())
    conf.set("spark.cores.max", "80") // TODO : change here
    conf.set("spark.akka.frameSize", "100000")
    conf.set("spark.executor.memory", "120g")    
    conf.set("spark.shuffle.file.buffer.kb", "1000")
    conf.setMaster("spark://dco-node040.dco.ethz.ch:7077")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")  
    conf.set("spark.driver.maxResultSize", "100g")
    val sc = new SparkContext(conf) 
    val dataPath = args(0)
    FewParamsLearning.loadData(80, sc)
    computeFrequencyPairsOfEntities()
    sc.stop()
  } 
 
    
  def learn(dataPath : String, max_product : Boolean) = {    

    //////////////////// SPARK context ///////////////////////////////
    val conf = new SparkConf().setAppName("EL LBP SPARK " + System.currentTimeMillis())
    conf.set("spark.cores.max", "128")
    conf.set("spark.executor.memory", "100g")
    conf.set("spark.akka.frameSize", "1000")
    conf.set("spark.shuffle.file.buffer.kb", "1000")
    conf.setMaster("spark://dco-node040.dco.ethz.ch:7077")
    conf.set("spark.driver.maxResultSize", "50g")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    conf.set("spark.storage.memoryFraction", "1")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.core.connection.ack.wait.timeout", "600")
    
    val sc = new SparkContext(conf)

    val NUM_ROUNDS = 500
    val NUM_WORKERS = 32
    val NUM_ITERS_PER_ROUND_PER_WORKER = 12500 // 10% of all data for one worker 
    val numeratorCtEta = 100000
    val numitorCtEta = 100000
    val regularizer = 0.00001
    
    val data = FewParamsLearning.loadData(NUM_WORKERS, sc)
    splittedTrainingDataRDD = data._1 
    validationData = data._2 
    testData = data._3 

    // SGD(numeratorCtEta, numitorCtEta, regularizer, NUM_ROUNDS, NUM_WORKERS, NUM_ITERS_PER_ROUND_PER_WORKER, max_product, sc)


    sc.stop()
  }  
  



  /*
   * For each pair (e_i,e_j) from the corpus, compute its frequency and
   * output just pairs with frequency >= 4.
   */
  def computeFrequencyPairsOfEntities() = {
    def mergeTLongHashSet(t1 : TLongHashSet, t2 : TLongHashSet) : TLongHashSet = {
      val nr = Math.random()
      System.err.println("Start one reduce : " + nr + " -- " + t1.size() + " -- " + t2.size())
      for (k2 <- t2.toArray()) {
        t1.add(k2)
      }
      t2.clear()
      System.err.println("Finished one reduce : " + nr + " -- " + t1.size() + " -- " + t2.size())
      t1
    }
    println("Compute frequent pairs of entities to consider for SGD ... ")
    val nrPairsAndFreqPairsMap = splittedTrainingDataRDD.sample(false, 0.97, 1).map(docs => {
      var nrPairs : Long = 0
      var nrDistPairs : Long = 0
      val hashtableRef = new MemClient
      val lambdaFrequentPairsToUse = new TLongHashSet()

      for (doc <- docs) {
        val inputPairs = doc._2
        for (i1 <- 0 until inputPairs.size) {
          for (i2 <- (i1 + 1) until inputPairs.size) {
            val y1 = inputPairs(i1)._2.intValue()
            val y2 = inputPairs(i2)._2.intValue()

            nrPairs += 1
            val (intVal, successGet) = hashtableRef.getF(y1, y2)
            if (!hashtableRef.containsF(y1, y2)) {
              nrDistPairs += 1
              hashtableRef.setF(y1,y2, 1)
            } else if (successGet) {
              hashtableRef.setF(y1, y2, intVal + 1)
              if (intVal + 1 >= 4) {
                lambdaFrequentPairsToUse.add(hashtableRef.getIntPairsKey(y1,y2))
              }
            }
          }
        }
      }
      ((nrPairs, nrDistPairs), lambdaFrequentPairsToUse)
    }).reduce((x,y) => ((x._1._1 + y._1._1, x._1._2 + y._1._2), mergeTLongHashSet(x._2, y._2)))

    val p = new java.io.PrintWriter(new java.io.File("pairs.out"))
    val freqPairsMap = nrPairsAndFreqPairsMap._2
    println("Num dist pairs with freq >= 4 : " + freqPairsMap.size())
    p.println("Format: E1\tE2\tFreq\tStoreKey")
    val hashtableRef = new MemClient
    for (k <- freqPairsMap.toArray()) {
      val (y1, y2) = hashtableRef.unfoldIntPairsKey(k)
      val (value,success) = hashtableRef.getF(y1, y2)
      if (success) {
        p.println(y1 + "\t" + y2 + "\t" + value + "\t" + k)
      }
    }
    hashtableRef.shutdown
    p.flush()
    p.close()
    println("\n\n ====================================== \n " +
        "Done computing freq pairs: nrTotalPairs = " + nrPairsAndFreqPairsMap._1._1  +
        " ; nr dist pairs = " + nrPairsAndFreqPairsMap._1._2)
    println("Resulted freq pair map of size " + freqPairsMap.size() + "\n\n\n")
  }

  /*
   * Init W : rhos with log p(y|x), lambdas all 0
   */
  def initW(numSGDWorkers : Int, sc : SparkContext) : (RDD[Array[String]], RDD[Array[Long]]) = {

    println("Initializing W ...")

    val mentionEntsFreqFile = "/mnt/cw12/Octavian/el-lbp-marina/marinah/wikipedia/mek-top-freq-crosswikis-plus-wikipedia-lowercase-top64.txt"

    println(" Init rhos from mentionEntsFreq Index " + mentionEntsFreqFile)
    val rhosKeys = sc.textFile(mentionEntsFreqFile, 64).mapPartitions(lineIter => {
      val map = new ArrayBuffer[String]
      val memcachedClient = new SGDWeightMemClient
      while (lineIter.hasNext) {
        val line = lineIter.next
        val elements = line.split("\t", 4)
        val mention = elements(0)
        val linkedDocs = elements(1).toInt
        val totalDocs = elements(2).toInt
        // Skip mentions contained in less than MINIMUM_COUNT docs.
        if (totalDocs >= 5 && linkedDocs > 0) {
          val candidates = new index.CandidatesList(elements(3), mention, AllIndexesWrapper.getAllIndexesBox.entIDToNameIndex)
          val entities = candidates.getCandidateEntities()
          // Write mention to entities index in memcached to be used by all workers
          memcachedClient.addMentionIndexEntry(mention, entities)
          for (entity <- entities) {
            val defaultRho = Math.log(candidates.getCandidateProbability(entity))
            memcachedClient.addRho(mention, entity, defaultRho)
            val ret = memcachedClient.getPartialRhoKey(mention, entity)
            map += ret
          }
        }
      }
      memcachedClient.shutdown
      Array(map.toArray).iterator
    }).cache

    val rhosSize = rhosKeys.map(x => x.length).reduce(_ + _)
    println(" Done init rhos. Size = " + rhosSize)


    val freqPairsFile = "/mnt/cw12/Octavian/el-lbp-marina/marinah/wikipedia/entityFreqPairs.txt"
    println(" Init lambdas from file " + freqPairsFile)
    val lambdasKeys = sc.textFile(freqPairsFile, 64).mapPartitions(lineIter => {
      val map = new ArrayBuffer[Long]()
      val entityLinksIndex = AllIndexesWrapper.getAllIndexesBox.wikipEntityCooccurrIndex
      val memcachedClient = new SGDWeightMemClient
      while (lineIter.hasNext) {
        val line = lineIter.next
        if (!line.startsWith("Format:")) {
          val tokens = line.split("\t")
          val x1 = tokens(0).toInt
          val x2 = tokens(1).toInt
          val cocitation = tokens(2).toInt
          if (x1 != x2 && cocitation >= 7) { /// TODO change this
            val p_hat_x1 = entityLinksIndex.getPopularity(x1)
            val p_hat_x2 = entityLinksIndex.getPopularity(x2)
            val indep_prior = (0.0 + entityLinksIndex.getTotalNumPairs()) / entityLinksIndex.getTotalNumLinks() * p_hat_x1 / (entityLinksIndex.getTotalNumLinks()  + 0.0) * p_hat_x2
            val defaultLambda = g * (Math.log(cocitation  + a * indep_prior) - h * Math.log(p_hat_x1) - h * Math.log(p_hat_x2))
            memcachedClient.addLambda(x1, x2, defaultLambda)
            val ret = memcachedClient.getPartialLambdaKey(x1, x2)
            map += ret
          }
        }
      }
      memcachedClient.shutdown
      Array(map.toArray).iterator
    }).cache

    val lambdasSize = lambdasKeys.map(x => x.length).reduce(_ + _)
    println(" Done init lambdas. Size = " + lambdasSize)


    // Build the entity -> other entities index.
    val nr = sc.textFile(freqPairsFile, numSGDWorkers).mapPartitions(lineIter => {
      var list = new ArrayBuffer[(Int,Int)]
      while (lineIter.hasNext) {
        val line = lineIter.next
        if (!line.startsWith("Format:")) {
          val tokens = line.split("\t")
          val x1 = tokens(0).toInt
          val x2 = tokens(1).toInt
          if (x1 != x2 && tokens(2).toInt >= 7) { /// TODO change this
            list += ((x1, x2))
            list += ((x2, x1))
          }
        }
      }
      list.iterator
    }).groupByKey.mapPartitions(entryIt => {
      val memcachedClient = new SGDWeightMemClient
      var sumSize : Long = 0
      var num = 0
      while (entryIt.hasNext) {
        val entry = entryIt.next
        sumSize += entry._2.size
        num += 1
        memcachedClient.addEntitiesIndexEntry(entry._1 , entry._2)
      }
      memcachedClient.shutdown
      Array((sumSize,num)).iterator
    }).reduce((x,y) => (x._1 + y._1, x._2 + y._2))
    println(" Done writing index in memory. avg num = " + (nr._1 / nr._2))

    println("Initializing W done.")
    (rhosKeys, lambdasKeys)
  }


  /* Local SGD in rounds for Piecewise learning:
   * - perform a number of rounds
   *    - in each round split the data on different nodes
   *    - each node does a full SGD on its partition of data
   *    - at the end of the round, all nodes average their results
   *    - the new averaged weight is passed to the next round to all nodes
   * - final weight vector is reported
   */
  def SGD(
      numeratorCtEta : Double,
      numitorCtEta : Double,
      regularizer : Double,
      numSGDRounds : Int,
      numSGDWorkers : Int,
      numIters : Int,
      max_product : Boolean,
      sc : SparkContext) {

    println()
    println("\n\n\n=======================================================")
    println("Start SGD with numeratorCtEta = " + numeratorCtEta +
        "; numitorCtEta = " + numitorCtEta + "; regularizer = " + regularizer +
        "; num workers = " + numSGDWorkers + "; num rounds = " + numSGDRounds +
        "; num iters per round per worker = " + numIters)

    val (rhosKeys, lambdasKeys) = initW(numSGDWorkers, sc)

    var s : Double = 1
    testParams(testData, s, rhosKeys, lambdasKeys, "Loopy Piecewise training after 0 rounds (before SGD start)", max_product, sc)

    for (round <- 1 to numSGDRounds) {
      println("\n\n ************* Start round " + round + " ***********\n\n")

      val sumDeltas = splittedTrainingDataRDD
      .map(localTrainingData =>
        LocalSGD(s, localTrainingData, numeratorCtEta, numitorCtEta, regularizer, numIters, round)
      )
      .map({case (rhos,lambdas) => (differenceFromOldVals(rhos), differenceFromOldVals(lambdas))})
      .reduce((x,y) => (mergeRhos(x._1,y._1), mergeLambdas(x._2,y._2)))

      // Update s.
      for (i <- 1 to numIters) {
        val eta = numeratorCtEta / (i + (round - 1) * numIters + numitorCtEta)
        s *= (1 - eta * regularizer)
      }

      finalMerge(sumDeltas._1, sumDeltas._2, numSGDWorkers)

      println("Finished SGD. s = " + s + ". Size updated rhos size = " + sumDeltas._1.size)
      println("Size updated lambdas size = " + sumDeltas._2.size)
      System.gc()

      // Do testing after a chunck of iterations
      if (round % 5 == 0) {
        println("\n\nStart testing at master ")
        testParams(testData, s, rhosKeys, lambdasKeys, "Loopy Piecewise training after " + round + " rounds", max_product, sc)
      }
      if (round % 20 == 0) {
        println("PRINT WEIGHTS TO STDOUT...")
        val w = extractSGDWeightsForEval(s, rhosKeys, lambdasKeys)
        w._1.print("/mnt/cw12/Octavian/el-lbp-marina/rhos_round_" + round)
        w._2.print("/mnt/cw12/Octavian/el-lbp-marina/lambdas_round_" + round)
        println("DONE PRINTING WEIGHTS TO STDOUT...")
      }
      System.gc()
    }

    lambdasKeys.unpersist(true)
    rhosKeys.unpersist(true)
    val memcached = new SGDWeightMemClient
    memcached.flushAll
    memcached.shutdown
    println("\n\n\n======== Done SGD ================\n\n\n\n")
  }

 /*
  * Local SGD on a single machine using Piecewise learning.
  *
  * Uses stochastic updates to exploit sparsity.
  * Reference: http://stronglyconvex.com/blog/sparse-l2.html
  * The update eq  w_{t+1} = w_t * (1 - alpha_t) - beta_t * x_t  can be rewritten as:
  *   - s_{t+1} = (1-alpha_t) * s_t
  *   - v_{t+1} = v_t - beta_t * x_t / s_{t+1}
  * v params are kept in rhos and lambdas vectors.
  */
  def LocalSGD(
      globalS : Double,
      localTrainingData : Array[(Int, Array[(String, Int, Array[Int])])],
      numeratorCtEta : Double,
      numitorCtEta : Double,
      regularizer : Double,
      numIters : Long,
      numRound : Int)
  : (TObjectDoubleHashMap[String], TLongDoubleHashMap) = {

    val rhos = new TObjectDoubleHashMap[String]
    val lambdas = new TLongDoubleHashMap

    val memcached = new SGDPiecewiseMemClient
    val workerID = memcached.generateWorkerID

    println("Start local SGD for worker = " + workerID)
    var s = globalS

    var wUpdatedSoFar : Long = 0
    var queriesToMemc : Long = 0
    var i : Int = 0
    while (i <= numIters) {
      i += 1
      val eta = numeratorCtEta / (i + (numRound - 1) * numIters + numitorCtEta)

      val randomDoc = localTrainingData((Math.random() * (localTrainingData.size - 1)).toInt)
      val inputPairs = randomDoc._2

      s *= (1 - eta * regularizer)
      // TODO maybe: Restart of s in case it gets too small

      val (weightsUpdatedThisPage, queriesToMemcache) = SGDStepPseudolikelihood(rhos, lambdas, eta, s, inputPairs, memcached)
      wUpdatedSoFar += weightsUpdatedThisPage
      queriesToMemc += queriesToMemcache

      if (i%100 == 1){
        val ps = rhos.size() + lambdas.size()
        println("Local SGD at worker " + workerID + " iter " + i +
            "; weightsUpdatedSoFar = " + wUpdatedSoFar +
            "; queries to memcached = " + queriesToMemc +
            "; rhos+lambdas size=" + ps)
      }
    }

    println("Finished local SGD for worker = " + workerID + " after iterations num = " + i)
    memcached.shutdown
    System.gc()
    (rhos, lambdas)
  }

  /*
   * This should be called just on pairs that one is sure are in memcache.
   */
  def getOldRhoValue(
      x : String,
      y : Int,
      rhos : TObjectDoubleHashMap[String],
      memcached : SGDPiecewiseMemClient) : Double =  {
    var oldVal : Double = 0
    if (rhos.containsKey(memcached.getPartialRhoKey(x, y))) {
      oldVal = rhos.get(memcached.getPartialRhoKey(x, y))
    } else {
      val v = memcached.finalizeRhoGetRequest(x, y)
      if (v != null) {
        oldVal = v.toString().toDouble // In memcached we have w_i which is v_i, without the s multiplying factor.
      }
    }
    if (oldVal > 15) {
      oldVal = 15
    }
    oldVal
  }

  /*
   * This should be called just on pairs that one is sure are in memcache.
   */
  def getOldLambdaValue(
      y1 : Int,
      y2 : Int,
      lambdas : TLongDoubleHashMap,
      memcached : SGDPiecewiseMemClient) : Double =  {
    var oldVal : Double = 0
    if (lambdas.containsKey(memcached.getPartialLambdaKey(y1,y2))) {
      oldVal = lambdas.get(memcached.getPartialLambdaKey(y1,y2))
    } else {
      val v = memcached.finalizeLambdaGetRequest(y1, y2)
      if (v != null) {
        oldVal = v.toString().toDouble
      }
    }
    if (oldVal > 15) {
      oldVal = 15
    }
    oldVal
  }

  /*
   * Get for all candidate entities for one name
   * all pairs of form
   * (entity, all entities that are in memcached with this entity)
   */
  def getLambdaParamsThatExistInMemcache(
      inputPairs : Array[(String,Int, Array[Int])],
      y_spaces : THashMap[String,Array[Int]],
      memcached : SGDPiecewiseMemClient) : TLongHashSet = {
    val res = new TLongHashSet
    for (i1 <- 0 until inputPairs.size) {
      val y1 = inputPairs(i1)._2
      val x1 = inputPairs(i1)._1
      val y1_space = y_spaces.get(x1)
      memcached.getEntitiesIndexEntry(y1_space, res)
    }
    res
  }

  def SendAllRequestsToMemcachedForSGDStepPiecewise(
      rhos : TObjectDoubleHashMap[String],
      lambdas : TLongDoubleHashMap,
      inputPairs : Array[(String,Int, Array[Int])],
      y_spaces : THashMap[String, Array[Int]],
      lambdaParamsToUpdate : TLongHashSet,
      memcached : SGDPiecewiseMemClient) : Long = {

    var nrParams : Long = 0
    for ((x,y,context) <- inputPairs) {
      val y_space = y_spaces.get(x)
      if (y_space.contains(y) && !rhos.containsKey(memcached.getPartialRhoKey(x, y))) {
        memcached.sendRhoGetRequest(x, y)
        nrParams += 1
      }
    }
    for ((x,y,context) <- inputPairs) {
      val y_space = y_spaces.get(x)
      for (y_prim <- y_space) {
        if (!rhos.containsKey(memcached.getPartialRhoKey(x, y_prim))) {
          memcached.sendRhoGetRequest(x, y_prim)
          nrParams += 1
        }
      }
    }
    for (i1 <- 0 until inputPairs.size) {
      for (i2 <- (i1 + 1) until inputPairs.size) {
        val y1 = inputPairs(i1)._2
        val y2 = inputPairs(i2)._2
        if (lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(y1, y2)) &&
            !lambdas.containsKey(memcached.getPartialLambdaKey(y1, y2))) {
          memcached.sendLambdaGetRequest(y1, y2)
          nrParams += 1
        }
      }
    }
    for (i1 <- 0 until inputPairs.size) {
      val y1 = inputPairs(i1)._2
      val x1 = inputPairs(i1)._1
      val y1_space = y_spaces.get(x1)

      for (i2 <- (i1 + 1) until inputPairs.size) {
        val y2 = inputPairs(i2)._2
        val x2 = inputPairs(i2)._1
        val y2_space = y_spaces.get(x2)

        for (y1_prim <- y1_space) {
          for (y2_prim <- y2_space) {
            if (lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(y1_prim, y2_prim)) &&
                !lambdas.containsKey(memcached.getPartialLambdaKey(y1_prim, y2_prim))) {
              memcached.sendLambdaGetRequest(y1_prim, y2_prim)
              nrParams += 1
            }
          }
        }
      }
    }
    nrParams
  }


  /*
   * One SGD update step for a single document using Piecewise learning.
   */
  def SGDStepPiecewise(
      rhos : TObjectDoubleHashMap[String],
      lambdas : TLongDoubleHashMap,
      eta : Double,
      s : Double,
      inputPairs : Array[(String,Int, Array[Int])],
      memcached : SGDPiecewiseMemClient) : (Long, Long) =  {

    val l = inputPairs.size
    var weightsUpdatedSoFar : Long = 0

    val y_spaces = memcached.getMentionIndexEntry(inputPairs.map(p => p._1))
    val lambdaParamsToUpdate = getLambdaParamsThatExistInMemcache(inputPairs, y_spaces, memcached)

    ///////////////////////////////////////////////////////////////
    // Send all requests for parameters to be updated to memcached.
    ///////////////////////////////////////////////////////////////
    val queriesToMemcache =
      SendAllRequestsToMemcachedForSGDStepPiecewise(
          rhos,
          lambdas,
          inputPairs,
          y_spaces,
          lambdaParamsToUpdate,
          memcached)

    //////////////////////////////////////////////////
    // Update rho rhoparams using numerators of p(y|x).
    //////////////////////////////////////////////////
    for ((x,y,context) <- inputPairs) {
      val gradient = 1.0 / l
      val y_space = y_spaces.get(x)
      if (y_space.contains(y)) {
        val oldVal = getOldRhoValue(x, y, rhos, memcached)
        weightsUpdatedSoFar += 1
        rhos.put(memcached.getPartialRhoKey(x, y), oldVal + eta / s  * gradient)
      }
    }

    //////////////////////////////////////////////////
    // Update rho params using numitors(Z function) of p(y|x).
    //////////////////////////////////////////////////
    for ((x,y,context) <- inputPairs) {
      val y_space = y_spaces.get(x)
      var Z = new java.math.BigDecimal(0.0)
      for (y_prim <- y_space) {
        val oldVal = getOldRhoValue(x, y_prim, rhos, memcached)
        val toAdd = Math.exp(1.0 / l * oldVal * s)
        try {
          Z = Z.add(new java.math.BigDecimal(toAdd))
        } catch {
          case e : Exception => {
            println(e.getMessage)
            println("Z = " + Z.doubleValue() + "; toAdd = " + toAdd + " ;rho = " +
                oldVal + " x = " + x + " y= " + y_prim)
          }
        }
      }

      for (y_prim <- y_space) {
        val oldVal = getOldRhoValue(x, y_prim, rhos, memcached)
        val gradient = 1.0 / l * Math.exp((1.0 / l) * oldVal * s) / Z.doubleValue()
        rhos.put(memcached.getPartialRhoKey(x, y_prim), oldVal - eta / s  * gradient)
        weightsUpdatedSoFar += 1
      }
    }

    //////////////////////////////////////////////////
    // Update lambda params using numerators of p(y|x).
    //////////////////////////////////////////////////
    for (i1 <- 0 until inputPairs.size) {
      for (i2 <- (i1 + 1) until inputPairs.size) {
        val y1 = inputPairs(i1)._2
        val y2 = inputPairs(i2)._2
        if (lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(y1, y2))) {
          val oldVal = getOldLambdaValue(y1, y2, lambdas, memcached)
          val gradient = 2.0 / (l * (l-1))
          lambdas.put(memcached.getPartialLambdaKey(y1, y2), oldVal + eta / s  * gradient)
          weightsUpdatedSoFar += 1
        }
      }
    }

    //////////////////////////////////////////////////
    // Update lambda params using numitors(Z function) of p(y|x).
    //////////////////////////////////////////////////
    for (i1 <- 0 until inputPairs.size) {
      val y1 = inputPairs(i1)._2
      val x1 = inputPairs(i1)._1
      val y1_space = y_spaces.get(x1)

      for (i2 <- (i1 + 1) until inputPairs.size) {
        val y2 = inputPairs(i2)._2
        val x2 = inputPairs(i2)._1
        val y2_space = y_spaces.get(x2)
        var Z : java.math.BigDecimal = new java.math.BigDecimal(0.0)

        for (y1_prim <- y1_space) {
          for (y2_prim <- y2_space) {
            if (lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(y1_prim, y2_prim))) {
              val oldVal = getOldLambdaValue(y1_prim, y2_prim, lambdas, memcached)
              val toAdd = Math.exp(2.0 / (l * (l-1)) * oldVal * s)
              try {
                Z = Z.add(new java.math.BigDecimal(toAdd))
              } catch {
                case e : Exception => {
                  println(e.getMessage)
                  println("Z = " + Z.doubleValue() + "; toAdd = " + toAdd +
                      " ;lambda = " + oldVal + " y1 = " + y1_prim + " y2 = " + y2_prim)
                }
              }
            }
          }
        }

        for (y1_prim <- y1_space) {
          for (y2_prim <- y2_space) {
            if (lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(y1_prim, y2_prim))) {
              val lam = getOldLambdaValue(y1_prim, y2_prim, lambdas, memcached)
              val gradient = 2.0 / (l * (l-1)) * Math.exp((2.0 / (l * (l-1))) * lam * s) / Z.doubleValue()
              lambdas.put(memcached.getPartialLambdaKey(y1_prim, y2_prim), lam - (eta / s) * gradient)
              weightsUpdatedSoFar += 1
            }
          }
        }
      }
    }
    memcached.finalizeAllGetAndSetRequests()
    (weightsUpdatedSoFar, queriesToMemcache)
  }


  def SendAllRequestsToMemcachedForSGDStepPseudolikelihood(
      rhos : TObjectDoubleHashMap[String],
      lambdas : TLongDoubleHashMap,
      inputPairs : Array[(String,Int, Array[Int])],
      y_spaces : THashMap[String, Array[Int]],
      lambdaParamsToUpdate : TLongHashSet,
      memcached : SGDPiecewiseMemClient) : Long = {

    var nrParams : Long = 0
    for ((x,y,context) <- inputPairs) {
      val y_space = y_spaces.get(x)
      if (y_space.contains(y) && !rhos.containsKey(memcached.getPartialRhoKey(x, y))) {
        memcached.sendRhoGetRequest(x, y)
        nrParams += 1
      }
    }
    for ((x,y,context) <- inputPairs) {
      val y_space = y_spaces.get(x)
      for (y_prim <- y_space) {
        if (!rhos.containsKey(memcached.getPartialRhoKey(x, y_prim))) {
          memcached.sendRhoGetRequest(x, y_prim)
          nrParams += 1
        }
      }
    }
    for (i1 <- 0 until inputPairs.size) {
      for (i2 <- (i1 + 1) until inputPairs.size) {
        val y1 = inputPairs(i1)._2
        val y2 = inputPairs(i2)._2
        if (lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(y1, y2)) &&
            !lambdas.containsKey(memcached.getPartialLambdaKey(y1, y2))) {
          memcached.sendLambdaGetRequest(y1, y2)
          nrParams += 1
        }
      }
    }
    for (i1 <- 0 until inputPairs.size) {
      val y1 = inputPairs(i1)._2
      val x1 = inputPairs(i1)._1
      val y1_space = y_spaces.get(x1)

      for (i2 <- 0 until inputPairs.size) {
        val y2 = inputPairs(i2)._2
        val x2 = inputPairs(i2)._1
        if (i2 != i1) {
          for (y1_prim <- y1_space) {
            if (lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(y1_prim, y2)) &&
                !lambdas.containsKey(memcached.getPartialLambdaKey(y1_prim, y2))) {
              memcached.sendLambdaGetRequest(y1_prim, y2)
              nrParams += 1
            }
          }
        }
      }
    }
    nrParams
  }


  /*
   * One SGD update step for a single document using Pseudolikelihood learning.
   */
  def SGDStepPseudolikelihood(
      rhos : TObjectDoubleHashMap[String],
      lambdas : TLongDoubleHashMap,
      eta : Double,
      s : Double,
      inputPairs : Array[(String,Int, Array[Int])],
      memcached : SGDPiecewiseMemClient) : (Long, Long) =  {

    val l = inputPairs.size
    var weightsUpdatedSoFar : Long = 0

    val y_spaces = memcached.getMentionIndexEntry(inputPairs.map(p => p._1))
    val lambdaParamsToUpdate = getLambdaParamsThatExistInMemcache(inputPairs, y_spaces, memcached)

    ///////////////////////////////////////////////////////////////
    // Send all requests for parameters to be updated to memcached.
    ///////////////////////////////////////////////////////////////
    val queriesToMemcache =
      SendAllRequestsToMemcachedForSGDStepPseudolikelihood(
          rhos,
          lambdas,
          inputPairs,
          y_spaces,
          lambdaParamsToUpdate,
          memcached)

    /*
     * rho(x,y) * 1 / l + \sum_j lambda_{y,yj} * 1 / binom(l,2)
     */
    def log_factor(x : String, y : Int) : Double = {
      var rez = 1.0 / l * getOldRhoValue(x, y, rhos, memcached) * s
      for ((xj,yj,context_j) <-  inputPairs) {
        if (xj != x && lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(yj, y))) {
          rez += 2.0 / (l * (l-1)) * getOldLambdaValue(yj, y, lambdas, memcached) * s
        }
      }
      if (rez > 15) {
        rez = 15
      }
      rez
    }

    //////////////////////////////////////////////////
    // Update rho params using numerators of p(y|x).
    //////////////////////////////////////////////////
    for ((x,y,context) <- inputPairs) {
      val gradient = 1.0 / l
      val y_space = y_spaces.get(x)
      if (y_space.contains(y)) {
        val oldVal = getOldRhoValue(x, y, rhos, memcached)
        weightsUpdatedSoFar += 1
        rhos.put(memcached.getPartialRhoKey(x, y), oldVal + eta / s  * gradient)
      }
    }

    //////////////////////////////////////////////////
    // Update lambda params using numerators of p(y|x).
    //////////////////////////////////////////////////
    for (i1 <- 0 until inputPairs.size) {
      for (i2 <- 0 until inputPairs.size) {
        val y1 = inputPairs(i1)._2
        val y2 = inputPairs(i2)._2
        if (i1 != i2 && lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(y1, y2))) {
          val oldVal = getOldLambdaValue(y1, y2, lambdas, memcached)
          val gradient = 2.0 / (l * (l-1))
          lambdas.put(memcached.getPartialLambdaKey(y1, y2), oldVal + eta / s  * gradient)
          weightsUpdatedSoFar += 1
        }
      }
    }

    //////////////////////////////////////////////////
    // Update rho params using numitors(Z function) of p(y|x).
    //////////////////////////////////////////////////
    for ((x,y,context) <- inputPairs) {
      val y_space = y_spaces.get(x)
      var Z = new java.math.BigDecimal(0.0)
      for (y_prim <- y_space) {
        val oldVal = log_factor(x,y_prim)
        val toAdd = Math.exp(oldVal)
        try {
          Z = Z.add(new java.math.BigDecimal(toAdd))
        } catch {
          case e : Exception => {
            println(e.getMessage)
            println("Z = " + Z.doubleValue() + "; toAdd = " + toAdd + " ;log factor = " +
                oldVal + " x = " + x + " y= " + y_prim)
          }
        }
      }

      for (y_prim <- y_space) {
        val oldVal = getOldRhoValue(x, y_prim, rhos, memcached)
        val log_fact = log_factor(x,y_prim)
        val gradient = 1.0 / l * Math.exp(log_fact) / Z.doubleValue()
        rhos.put(memcached.getPartialRhoKey(x, y_prim), oldVal - eta / s  * gradient)
        weightsUpdatedSoFar += 1

        for ((xj,yj,context_j) <-  inputPairs) {
          if (xj != x && lambdaParamsToUpdate.contains(memcached.getPartialLambdaKey(yj, y_prim))) {
            val lam = getOldLambdaValue(y_prim, yj, lambdas, memcached)
            val gradient = 2.0 / (l * (l-1)) * Math.exp(log_fact) / Z.doubleValue()
            lambdas.put(memcached.getPartialLambdaKey(yj, y_prim), lam - (eta / s) * gradient)
            weightsUpdatedSoFar += 1
          }
        }
      }
    }

    memcached.finalizeAllGetAndSetRequests()
    (weightsUpdatedSoFar, queriesToMemcache)
  }








  /*
   * Compute difference of the sparse updates and the old elements from the master (from memcached).
   */
  def differenceFromOldVals(rhos : TObjectDoubleHashMap[String]) : TObjectDoubleHashMap[String] = {
    val memcached = new SGDPiecewiseMemClient
    val it = rhos.keySet().iterator()
    val currKeys = new ArrayBuffer[String]
    while (it.hasNext()) {
      val key = it.next()
      memcached.sendRhoGetRequest(key)
      currKeys += key
      if (currKeys.size == 5000) {
        for (k <- currKeys) {
          rhos.put(k, rhos.get(k) - memcached.finalizeRhoGetRequest(k))
        }
        currKeys.clear
      }
    }
    memcached.finalizeAllGetAndSetRequests()
    memcached.shutdown
    rhos
  }

  def differenceFromOldVals(lambdas : TLongDoubleHashMap) : TLongDoubleHashMap = {
    val memcached = new SGDPiecewiseMemClient
    val it = lambdas.keySet().iterator()
    val currKeys = new ArrayBuffer[Long]
    while (it.hasNext()) {
      val key = it.next()
      memcached.sendLambdaGetRequest(key)
      currKeys += key
      if (currKeys.size == 5000) {
        for (k <- currKeys) {
          lambdas.put(k, lambdas.get(k) - memcached.finalizeLambdaGetRequest(k))
        }
        currKeys.clear
      }
    }
    memcached.finalizeAllGetAndSetRequests()
    memcached.shutdown
    lambdas
  }

  /*
   * Computes sum of differences of the deltas of updates.
   */
  def mergeRhos(
      w1 : TObjectDoubleHashMap[String],
      w2 : TObjectDoubleHashMap[String])
    : TObjectDoubleHashMap[String] = {
    System.err.println("Start one Rho merge : " + w1.size() + " -- " + w2.size())
    val keys = w2.keySet()
    val it = keys.iterator()
    while (it.hasNext()) {
      val k = it.next()
      var value = w2.get(k)
      if (w1.containsKey(k)) {
        value += w1.get(k)
      }
      w1.put(k, value)
    }
    w2.clear()
    System.err.println("Done one Rho merge : " + w1.size() + " -- " + w2.size())
    w1
  }

  def mergeLambdas(
      w1 : TLongDoubleHashMap,
      w2 : TLongDoubleHashMap)
    : TLongDoubleHashMap = {
    System.err.println("Start one Lambda merge : " + w1.size() + " -- " + w2.size())
    val keys = w2.keySet()
    val it = keys.iterator()
    while (it.hasNext()) {
      val k = it.next()
      var value = w2.get(k)
      if (w1.containsKey(k)) {
        value += w1.get(k)
      }
      w1.put(k, value)
    }
    w2.clear()
    System.err.println("Done one Lambda merge : " + w1.size() + " -- " + w2.size())
    w1
  }


  /*
   * Replace old W by adding the average of all (sparse) updated.
   * Also, write results back to memcache.
   */
  def finalMerge(
      rhos: TObjectDoubleHashMap[String],
      lambdas: TLongDoubleHashMap,
      numWorkers : Int) = {

    println("Started final merge .")
    val memcached = new SGDPiecewiseMemClient
    val it = rhos.keySet().iterator()

    val currKeys = new ArrayBuffer[String]
    while (it.hasNext()) {
      val key = it.next()
      memcached.sendRhoGetRequest(key)
      currKeys += key
      if (currKeys.size == 5000) {
        for (k <- currKeys) {
          val finalV = memcached.finalizeRhoGetRequest(k) + (rhos.get(k) / numWorkers)
          memcached.sendRhoSetRequest(k, finalV)
        }
        currKeys.clear
      }
    }
    memcached.finalizeAllGetAndSetRequests()

    val it2 = lambdas.keySet().iterator()
    val currKeys2 = new ArrayBuffer[Long]
    while (it2.hasNext()) {
      val key = it2.next()
      memcached.sendLambdaGetRequest(key)
      currKeys2 += key
      if (currKeys2.size == 5000) {
        for (k <- currKeys2) {
          val finalW = memcached.finalizeLambdaGetRequest(k) + (lambdas.get(k) / numWorkers)
          memcached.sendLambdaSetRequest(k, finalW)
        }
        currKeys2.clear
      }
    }
    memcached.finalizeAllGetAndSetRequests()
    memcached.shutdown
    println("Done final merge ")
  }


  /*
   * Bring all weights from the memcached to a map structure in memory s.t.
   * they can be used for evaluation.
   */
  def extractSGDWeightsForEval(
      s : Double,
      rhosKeys : RDD[Array[String]],
      lambdasKeys : RDD[Array[Long]])
  : (OptimizedRhosMap, OptimizedLambdasMap) = {

    def mergeOptimizedLambdasMap(t1 : OptimizedLambdasMap, t2 : OptimizedLambdasMap) : OptimizedLambdasMap = {
      val nr = Math.random()
      System.err.println("Start one reduce : " + nr + " -- " + t1.getSize() + " -- " + t2.getSize())
      val it2 = t2.map.iterator()
      while (it2.hasNext()) {
        it2.advance()
        t1.putLambdaKey(it2.key(), it2.value())
      }
      t2.clear()
      System.err.println("Finished one reduce : " + nr + " -- " + t1.getSize() + " -- " + t2.getSize())
      t1
    }
    def mergeOptimizedRhosMap(t1 : OptimizedRhosMap, t2 : OptimizedRhosMap) : OptimizedRhosMap = {
      val nr = Math.random()
      System.err.println("Start one reduce : " + nr + " -- " + t1.getSize() + " -- " + t2.getSize())
      val it2 = t2.map.keySet().iterator()
      while (it2.hasNext()) {
        val k2 = it2.next()
        t1.putRhoKey(k2, t2.getRhoKey(k2))
      }
      t2.clear()
      System.err.println("Finished one reduce : " + nr + " -- " + t1.getSize() + " -- " + t2.getSize())
      t1
    }

    println("Start extracting for eval rhos at master ")
    val rhos = rhosKeys.map(rhoTable => {
      val memcached = new SGDWeightMemClient
      val rez = new OptimizedRhosMap(rhoTable.length)
      for (k <- rhoTable) {
        rez.putRhoKey(k, s * memcached.getWithRhoKey(k, -1)._1)
      }
      memcached.shutdown
      rez
    }).reduce((x,y) => mergeOptimizedRhosMap(x,y))
    println("Done extracting rhos for eval weights at master . Size = " + rhos.getSize())

    println("Start extracting for eval lambdas at master ")
    val lambdas = lambdasKeys.map(lambdaTable => {
      val memcached = new SGDWeightMemClient
      val rez = new OptimizedLambdasMap(lambdaTable.length)
      for (k <- lambdaTable) {
        rez.putLambdaKey(k, s * memcached.getWithLambdaKey(k, -1)._1)
      }
      memcached.shutdown
      rez
    }).reduce((x,y) => mergeOptimizedLambdasMap(x,y))
    println("Done extracting lambdas for eval weights at master . Size = " + lambdas.getSize())
    (rhos,lambdas)
  }

  // Test the accuracy of w.
  def testParams( ////////////// TODO: test in parallel , rewrite full scorer to use memcached, instead of OptimizedRhosMap and OptimizedLambdasMap
      testData: Array[(Int, Array[(String, Int, Array[Int])])],
      s : Double,
      rhosKeys : RDD[Array[String]],
      lambdasKeys : RDD[Array[Long]],
      banner : String,
      max_product : Boolean,
      sc : SparkContext) = {


    var w = extractSGDWeightsForEval(s, rhosKeys, lambdasKeys)
    println("EVAL : params size : s = " + s + " rhos = " + w._1.map.size() + " ; lambdas = " + w._2.map.size)

    EvalOnDatasets.evalAllDatasets(true, testData, new ScorerWeights(a,f,g,h,ss,b), w, banner, true, max_product, sc)

    w._1.clear
    w._2.clear
    System.gc()
  }

}