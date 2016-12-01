package learning
import scala.io.Source
import index.AllIndexesBox
import eval.VerifyEDLBPForListOfWords
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import index.MentionEntitiesFrequencyIndex
import loopybeliefpropagation.ScorerFull
import eval.EvalOnDatasets
import utils.Normalizer
import loopybeliefpropagation.ScorerWeights
import gnu.trove.map.hash.TObjectDoubleHashMap
import loopybeliefpropagation.ScorerWeights
import scala.collection.mutable.ArrayBuffer
import index.MentEntsFreqIndexWrapper
import loopybeliefpropagation.RhoCache

object AllIndexesWrapper {
  private val indexesToLoad = "mentionEntsFreqIndex,entIDToNameIndex,entNameToIDIndex, redirectIndex, wikipEntityCooccurrIndex, wordFreqDict,wordEntityProbs"
  private val allIndexes = new AllIndexesBox(indexesToLoad)
  
  def getAllIndexesBox : AllIndexesBox = allIndexes
}
 
/* General class that loads the training instances (x_i,y_i)_{i=1..N} and
 * uses AdaGrad parallel SGD to train the proper parameters by calling a specific update function
 * corresponding to a specific training method (StructSVM, piecewise training, etc).
 */
object FewParamsLearning {

  // Training data in form of (pageid, page_list_mention_entity) spitted per machines.
  var splittedTrainingDataRDD : RDD[Array[(Int, Array[(String,Int, Array[Int])])]] = null
  var validationData : Array[(Int, Array[(String,Int, Array[Int])])] = null 
  var testData : Array[(Int, Array[(String,Int, Array[Int])])] = null 

  val NUM_WORKERS = 32
 
  
  // Load test and training data.
  def loadData(
      numWorkers : Int, 
      sc : SparkContext) 
  : (RDD[Array[(Int, Array[(String, Int, Array[Int])])]],
      Array[(Int, Array[(String, Int, Array[Int])])],  
      Array[(Int, Array[(String, Int, Array[Int])])]) = {

    def loadSingleLine(line : String) : (Int, Array[(String, Int, Array[Int])]) = {
      val tokens = line.split("\t")
      val pageId = tokens(0).split(",").last.toInt
      val pageAnchors = new ArrayBuffer[(String,Int, Array[Int])]
      
      var i = 1
      while ( i < tokens.length) {
        val mention = Normalizer.normalizeLowercase(tokens(i).trim())
        i += 1
        val entity = Integer.parseInt(tokens(i))
        i += 1
        pageAnchors += ((mention, entity, null)) /// TODO: fill in the context here
      }
      (pageId, pageAnchors.toArray)    
    }    
    
    val dirPath = "/mnt/cw12/Octavian/el-lbp-marina/marinah/wikipedia/"
	println("********** Loading training, dev , test data from " + dirPath + "...")
    
    val devPath = dirPath + "anchorsListFromEachWikilinksDoc.txt_dev_learning," + dirPath + "anchorsListFromEachWikiPage.txt_dev_learning"
    val devDataRDD = sc.textFile(devPath, numWorkers).map(line => loadSingleLine(line))
    val splittedTrainingDataRDD = devDataRDD.repartition(numWorkers).mapPartitions(x => Iterator(x.toArray))
    val dataCount = splittedTrainingDataRDD.map(x => x.size).reduce(_ + _)
    
    val valPath = dirPath + "anchorsListFromEachWikilinksDoc.txt_val," + dirPath + "anchorsListFromEachWikiPage.txt_val"
    val validationData = sc.textFile(valPath).map(line => loadSingleLine(line)).takeSample(false, 1000, 10)
    
    val testPath = dirPath + "anchorsListFromEachWikilinksDoc.txt_test," + dirPath + "anchorsListFromEachWikiPage.txt_test"
    val testData = sc.textFile(testPath).map(line => loadSingleLine(line)).takeSample(false, 10000, 1) //.collect //.takeSample(false, 2000, 1)
/////////////////////////////////////////////////////////////////// TODO TODO TODO : remove this sample above !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    
    println("*********** " + "Loaded training data. Size=" + dataCount + 
	    "  ; test size=" + testData.size + "  ; validation size=" + validationData.size)
	    
	(splittedTrainingDataRDD, validationData, testData)
  }  
  
  ////////////////// Learning phase ////////////////////////////////
  def learn(dataPath : String, max_product : Boolean) = {    
    val conf = new SparkConf().setAppName("EL LBP SPARK Learning few params " + Math.random())
    conf.set("spark.cores.max", "112")
    conf.set("spark.executor.memory", "100g")
    conf.set("spark.akka.frameSize", "1000")
    conf.set("spark.shuffle.file.buffer.kb", "1000")
    conf.set("spark.driver.maxResultSize", "50g")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    conf.set("spark.storage.memoryFraction", "1")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.core.connection.ack.wait.time10out", "600")
    
    val sc = new SparkContext(conf)

/*
    val data = loadData(NUM_WORKERS, sc)
    splittedTrainingDataRDD = data._1
    validationData = data._2
    testData = data._3
*/

    ///////////////////////////////
    /*
     * Just eval on all datasets.
     */

    
    
    
// Test best params:    
    
	  val weights = new ScorerWeights
	  weights.g = 0.5
	  weights.b = 0.075
	  weights.delta_w_e = 1.0
	  weights.xi = 0.5
	  weights.delta_cocit = 0.5
	  weights.cocit_e_e_param = 0.01

	  EvalOnDatasets.evalAllDatasets(
			  allSets = true,
			  null,
			  weights,
			  w = null,
			  "TEST w = " + weights,
			  fullPrint = false,
			  max_product,
			  sc)      
    
    
    System.exit(1)
    
    
    
    
    
    
    
    
    
    
    
  class BestParams(val cocit_e_e_param : Double, val xi_context : Double, val delta_cocit :  Double, val g : Double, val b : Double) {
      override def toString() : String = {
        "b = " + b + " xi_context = " + xi_context + " g = " + g +
        "; delta_cocit = " + delta_cocit +
        " cocit_e_e_param = " + cocit_e_e_param
      }
    }  

  var bestParamsAIDA : (BestParams, Double) = (null, 0.0)  
  var bestParamsCombined : (BestParams, Double) = (null, 0.0)  
    
  val delta_w_e = 1.0
  
  var nr_iter = 0
  
  for (delta_cocit <- List(1.5, 0.5, 1.0)) {
  for (g <- List(1.0, 0.5, 1.5, 2.0)) {
  for (cocit_e_e_param <- List(0.01, 0.1, 1.0)) {
  for (xi_context <- List(0.25, 0.1, 0.5)) {
  for (b <- List(0.06, 0.0, 0.03, 0.05, 0.075, 0.1)) {
    
    nr_iter += 1

    println("\n\n\n\n\n\n***************************************************************")
    println("************* b = " + b + " delta_w_e = " + delta_w_e + " xi_context = " + xi_context + " g = " + g + "; delta_cocit = " + delta_cocit +
        " cocit_e_e_param = " + cocit_e_e_param + " *******************")
    println("***************************************************************\n")

    if (b == 0) {
      println("No p(context|e) contribution")
    }
    
    var weights = new ScorerWeights
    weights.g = g
    weights.b = b
    weights.delta_w_e = delta_w_e
    weights.xi = xi_context
    weights.delta_cocit = delta_cocit
    weights.cocit_e_e_param = cocit_e_e_param
          
    val results = 
      EvalOnDatasets.evalAllDatasets(
        allSets = false,
        null,
        weights,
        w = null,
        "TEST WITH b = " + b + " delta_w_e = " + delta_w_e + " xi_context = " + xi_context + " g = " + g  + "; delta_cocit = " 
        + delta_cocit + " cocit_e_e_param = " + cocit_e_e_param,
        fullPrint = false,
        max_product,
        sc)
        
     val params = new BestParams(cocit_e_e_param, xi_context, delta_cocit, g, b)
     if (results._1 > bestParamsAIDA._2) {
       bestParamsAIDA = (params, results._1)
     }
     if (results._2 > bestParamsCombined._2) {
       bestParamsCombined = (params, results._2)
     }
       
     
     if (nr_iter % 20 == 0) {
       testGlobal(nr_iter)
     }
  }
  }
  }
  }
  }

  
  def testGlobal(n : Int) {
	  println("\n\n\n ================ After " + n + " iters ================================ ")
	  println(" BEST AIDA TEST A PARAMS after " + n + " iters : " + bestParamsAIDA._1)
	  println(" BEST COMBINED PARAMS after " + n + " iters : " + bestParamsCombined._1)

	  println("\n\n $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ ")
	  var weights = new ScorerWeights
	  weights.g = bestParamsAIDA._1.g
	  weights.b = bestParamsAIDA._1.b
	  weights.delta_w_e = delta_w_e
	  weights.xi = bestParamsAIDA._1.xi_context
	  weights.delta_cocit = bestParamsAIDA._1.delta_cocit
	  weights.cocit_e_e_param = bestParamsAIDA._1.cocit_e_e_param

	  EvalOnDatasets.evalAllDatasets(
			  allSets = true,
			  null,
			  weights,
			  w = null,
			  "TEST WITH BEST AIDA params " + bestParamsAIDA._1,
			  fullPrint = true,
			  max_product,
			  sc)          

	  println("\n\n $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ ")
	  weights = new ScorerWeights
	  weights.g = bestParamsCombined._1.g
	  weights.b = bestParamsCombined._1.b
	  weights.delta_w_e = delta_w_e
	  weights.xi = bestParamsCombined._1.xi_context
	  weights.delta_cocit = bestParamsCombined._1.delta_cocit
	  weights.cocit_e_e_param = bestParamsCombined._1.cocit_e_e_param
			
      EvalOnDatasets.evalAllDatasets(
          true,
          null,
          weights,
          w = null,
          "TEST WITH BEST COMBINED WIKI + AIDA params " + bestParamsCombined._1,
          fullPrint = false,
          max_product,
          sc)          
    }
        
    sc.stop
        
    System.exit(0)
    ////////////////////////////////
    
    
    // SGD(0.001, 0.001, 5, 32, 5000, sc)    

    
    
    ///////////////////////////////
    /*
     * Testing without learning. Grid search to find param a.
     */
    // findIndepPriorConstAndTestForModelWihoutLearnedParams(max_product, sc)
    ///////////////////////////////

    
    ///////////////////////////////
    /*
     * Train and test and report stats on 100 points. The loss should decrease.
     */
//    trainAndTestOnSmallSet(max_product, sc)
    ///////////////////////////////
    
    
    ///////////////////////////////    
    /*
     * 3 parameters learning with AdaGrad
     */
    findSGDParametersAndStartSGD(NUM_ITERS = 5000, max_product, sc)
    ////////// sc.stop()
    ///////////////////////////////
    
   
  }  
  

  
  /*
   * computes derivates and value of log p(y_i | y_{-i},x)
   */
  def logFactorAndItsDerivatives(
      x : String, 
      y : Int,
      scorer : ScorerFull, 
      inputPairs : Array[(String, Int, Array[Int])],
      justLoss : Boolean) : (Double, TObjectDoubleHashMap[String]) = {
    
    val l = inputPairs.length
    var log_factor = scorer.scorerWeights.f * scorer.rho(x, y, false)
   
    var partial_derivatives = new TObjectDoubleHashMap[String]
    if (!justLoss) {
      partial_derivatives.put("a", 0.0)
      partial_derivatives.put("f", scorer.rho(x, y, false))
      partial_derivatives.put("g", 0.0)
      partial_derivatives.put("h", 0.0)
    }
    
    for ((xj,yj,context_j) <-  inputPairs) {
      val cocitation = scorer.cocitation(y, yj)
      val indep_prior = scorer.independence_prior(y, yj)
      
      if (xj != x && cocitation  + scorer.scorerWeights.a * indep_prior > 0 &&
          scorer.p_hat(y) > 0 && scorer.p_hat(yj) > 0) {
        val lambda = scorer.lambda(y, yj, false)
        log_factor += scorer.scorerWeights.g * 2.0 / (l-1) * lambda
        if (!justLoss) {
          var part_a = partial_derivatives.get("a")
          part_a += scorer.scorerWeights.g * 2.0 / (l-1) * indep_prior / (cocitation  + scorer.scorerWeights.a * indep_prior)
          partial_derivatives.put("a", part_a)
          
          var part_g = partial_derivatives.get("g")
          part_g += 2.0 / (l-1) * lambda
          partial_derivatives.put("g", part_g)

          var part_h = partial_derivatives.get("h")
          part_h += scorer.scorerWeights.g * 2.0 / (l-1) * (-Math.log(scorer.p_hat(y)) - Math.log(scorer.p_hat(yj)))
          partial_derivatives.put("h", part_h)
        }
      }
    }
    if (log_factor > 10.0) { // Discard it
      log_factor = 0.0
      if (!justLoss) {
        partial_derivatives.put("a", 0.0)
        partial_derivatives.put("f", 0.0)
        partial_derivatives.put("g", 0.0)
        partial_derivatives.put("h", 0.0)        
      }
    }
    (log_factor, partial_derivatives)
  }


  /*
   * Log-likelihood loss function to be maximized and its partial derivatives.
   */
  def lossAndDerivates(w : ScorerWeights,
      docs : Array[(Int, Array[(String, Int, Array[Int])])], 
      reg : Double) : (Double, TObjectDoubleHashMap[String]) = {
    
    val allIndexes = AllIndexesWrapper.getAllIndexesBox
    
    var loss_fct = 0.0
    var partial_derivatives = new TObjectDoubleHashMap[String]
    partial_derivatives.put("a", 0.0)
    partial_derivatives.put("f", 0.0)
    partial_derivatives.put("g", 0.0)
    partial_derivatives.put("h", 0.0)
    partial_derivatives.put("s", 0.0)
    partial_derivatives.put("b", 0.0)
    
    for ((dociD, inputPairs) <- docs) {
      val l = inputPairs.length
      val rhoCache = new RhoCache
      if (l > 1 && l <= 25) { ///////////////////////////////////// TODOTODOTODO: remove this 25 !!!
        val mentions = new ArrayBuffer[String]
        val entities = new ArrayBuffer[Int]
        val mentEntsCache = new MentEntsFreqIndexWrapper(allIndexes, rhoCache, mentions.toArray, null, w)
        
        for (p <- inputPairs) {
          mentions += p._1
          entities += p._2
        }
    
        // Scorer used to compute features and mix with old weights.
        val contexts = null
        val scorer = new ScorerFull(mentions.toArray, entities.toArray, contexts, allIndexes, rhoCache, mentEntsCache, w)
        val EPS = 0.00001
        
        val s_plus_eps_weights = w.clone
        s_plus_eps_weights.s += EPS
        val scorer_plus_s = new ScorerFull(mentions.toArray, entities.toArray, contexts, allIndexes, rhoCache, mentEntsCache, s_plus_eps_weights)
        
        val b_plus_eps_weights = w.clone
        b_plus_eps_weights.b += EPS
        val scorer_plus_b = new ScorerFull(mentions.toArray, entities.toArray, contexts, allIndexes, rhoCache, mentEntsCache, b_plus_eps_weights)
        
        for ((x,y,context) <- inputPairs) {
          val y_space = mentEntsCache.getCandidateEntities(x)
          if (y_space.contains(y)) { // To avoid infinite values in the log_factor.
            loss_fct -= (reg / 2.0) * ((w.a - 0.5) * (w.a - 0.5) + (w.f - 10.0) * (w.f - 10.0) + (w.g - 10.0) * (w.g - 10.0) +
                (w.h - 0.5) * (w.h - 0.5) + (w.s - 1.0) * (w.s - 1.0)  + (w.b - 0.0) * (w.b - 0.0))
                
            var part_a = partial_derivatives.get("a")
            partial_derivatives.put("a", part_a - reg * (w.a - 0.5))

            var part_f = partial_derivatives.get("f")
            partial_derivatives.put("f", part_f - reg * (w.f - 10.0))

            var part_g = partial_derivatives.get("g")
            partial_derivatives.put("g", part_g - reg * (w.g - 10.0))

            var part_h = partial_derivatives.get("h")
            partial_derivatives.put("h", part_h - reg * (w.h - 0.5))

            var part_s = partial_derivatives.get("s")
            partial_derivatives.put("s", part_s - reg * (w.s - 1.0))

//            //partial_derivatives("b") -= reg * (w.b - 1.0)
            
            val deriv = logFactorAndItsDerivatives(x, y, scorer, inputPairs, false)
            loss_fct += 1.0 / l * deriv._1
            
            part_a = partial_derivatives.get("a")
            partial_derivatives.put("a", part_a + 1.0 / l * deriv._2.get("a"))
            
            part_f = partial_derivatives.get("f")
            partial_derivatives.put("f", part_f + 1.0 / l * deriv._2.get("f"))

            part_g = partial_derivatives.get("g")
            partial_derivatives.put("g", part_g + 1.0 / l * deriv._2.get("g"))

            part_h = partial_derivatives.get("h")
            partial_derivatives.put("h", part_h + 1.0 / l * deriv._2.get("h"))
            
            
            // Partial derivative of s and b : This derivative is very ugly, so we use finite differences:
            val deriv_plus_s = logFactorAndItsDerivatives(x, y, scorer_plus_s, inputPairs, true)
            val finite_dif_s = (deriv_plus_s._1  - deriv._1) / EPS
            
            part_s = partial_derivatives.get("s")
            partial_derivatives.put("s", part_s + 1.0 / l * finite_dif_s)            

/* 
            val deriv_plus_b = logFactorAndItsDerivatives(x, y, scorer_plus_b, inputPairs, true)
            val finite_dif_b = (deriv_plus_b._1  - deriv._1) / EPS
            partial_derivatives("b") += 1.0 / l * finite_dif_b
            ////////// Done computing partial derivative of s
*/
            
            var Z = 0.0
            var numeratorA = 0.0
            var numeratorF = 0.0
            var numeratorG = 0.0
            var numeratorH = 0.0
            var numeratorS = 0.0
//            var numeratorB = 0.0

            var max_log_factor = 0.0
            for (y_prim <- y_space) {
              if (scorer.mentEntsCache.getCandidateProbability(x, y_prim) > 0) {
                val deriv_prim = logFactorAndItsDerivatives(x, y_prim, scorer, inputPairs, false)
                if (deriv_prim._1 > max_log_factor) {
                  max_log_factor = deriv_prim._1
                }
              }
            }
            for (y_prim <- y_space) {
              if (scorer.mentEntsCache.getCandidateProbability(x, y_prim) > 0) {
                val deriv_prim = logFactorAndItsDerivatives(x, y_prim, scorer, inputPairs, false)
                val factor = Math.exp(deriv_prim._1 - max_log_factor + 12) // Trick to avoid underflow errors.
                Z += factor
                numeratorA += factor * deriv_prim._2.get("a")
                numeratorF += factor * deriv_prim._2.get("f")
                numeratorG += factor * deriv_prim._2.get("g")
                numeratorH += factor * deriv_prim._2.get("h")
                
                // Partial derivative of b,s : This derivative is very ugly, so we use finite differences:
                val deriv_prime_plus_s = logFactorAndItsDerivatives(x, y_prim, scorer_plus_s, inputPairs, true)
                val finite_prime_dif_s = (deriv_prime_plus_s._1  - deriv_prim._1) / EPS
                numeratorS += factor * finite_prime_dif_s

/*                
                val deriv_prime_plus_b = logFactorAndItsDerivatives(x, y_prim, scorer_plus_b, inputPairs, true)
                val finite_prime_dif_b = (deriv_prime_plus_b._1  - deriv_prim._1) / EPS
//                numeratorB += factor * finite_prime_dif_b                
                ////////// Done computing partial derivative of b,s
*/
                
              }
            }
            if (Z > 0) {
              loss_fct -= 1.0 / l * Math.log(Z)
            
              part_a = partial_derivatives.get("a")
              partial_derivatives.put("a", part_a - 1.0 / l * numeratorA / Z)

              part_f = partial_derivatives.get("f")
              partial_derivatives.put("f", part_f - 1.0 / l * numeratorF / Z)

              part_g = partial_derivatives.get("g")
              partial_derivatives.put("g", part_g - 1.0 / l * numeratorG / Z)

              part_h = partial_derivatives.get("h")
              partial_derivatives.put("h", part_h - 1.0 / l * numeratorH / Z)

              part_s = partial_derivatives.get("s")
              partial_derivatives.put("s", part_s - 1.0 / l * numeratorS / Z)
              
//              partial_derivatives("b") -= 1.0 / l * numeratorB / Z
            }      
          }
        }
      }
    }

    (loss_fct, partial_derivatives)
  }
  
  /*
   * Train and test and report stats on 100 points. The loss should decrease.
   */
  def trainAndTestOnSmallSet(max_product : Boolean, sc : SparkContext) = {
    println("\n\n\n=========================================")
    println("====== Train and test on small set. Start grid search to find best reg and eta ==================")
    println("===============================================")
    println("\n\n\n")
    
    val regularizers = List(0.001, 0.01, 0.1)
    val etas = List(0.01, 0.1, 1.0)//0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0, 5.0)
    
    val paramsList = for (reg <- regularizers; eta <- etas; adaGrad <- List(true)) yield (reg,eta, adaGrad)
      
    val oneShardData = util.Random.shuffle(validationData.toList).take(30).toArray // 5 random points
    val trainingDataBroadcast = sc.broadcast(oneShardData)
    
    val params = sc.parallelize(paramsList, paramsList.size)
      .map{ case (reg,eta, adaGrad) => {
        val data = trainingDataBroadcast.value
        println("Start for reg: " + reg + " eta: " + eta + " adaGrad: " + adaGrad)
        var w = new ScorerWeights
        
        val loss_evals0 = lossAndDerivates(w, data, reg)
        println("Loss after " + 0 + " rounds = " + loss_evals0._1 +
            " ; loss derivative = " + loss_evals0._2.get("g") +
            " ;for reg: " + reg + " eta: " + eta + " adaGrad: " + adaGrad +
            " ; Weights a = " + w.a + "; f = " + w.f + "; g = " + w.g + "; h = " + w.h + "; s = " + w.s + "; b = " + w.b)
        
        for (iter <- 1 to 700) {        
          w = SingleThreadSGD(w, iter, trainingDataBroadcast.value, eta, reg, trainingDataBroadcast.value.size, adaGrad)
          
          val loss_evals = lossAndDerivates(w, data, reg)

          println("Loss after " + iter + " rounds = " + loss_evals._1 +
              " ; loss derivative = " + loss_evals._2.get("g") +
              " ;for reg: " + reg + " eta: " + eta + " adaGrad: " + adaGrad +
              " ; Weights a = " + w.a + "; f = " + w.f + "; g = " + w.g + "; h = " + w.h + "; s = " + w.s + "; b = " + w.b)
        }
        (w, reg, eta, adaGrad)
      }}.collect
    
    val results = params.map{case (w, reg, eta, adaGrad) => {
      val f1 = EvalOnDatasets.evalWikiValidationData(
          oneShardData, 
          w,
          null,
          "Validation set for reg = " + reg + " eta = " + eta + " adaGrad : " + adaGrad, 
          fullPrint = false, 
          max_product,
          sc)
          
      val str = "Loopy training for reg = " + reg + " eta = " + eta + " adaGrad : " + adaGrad + 
        	"; Weights a = " + w.a + "; f = " + w.f + "; g = " + w.g + "; h = " + w.h + "; s = " + w.s  + "; b = " + w.b +
        	" adagrad iters = " + w.num_grads + "; Gradient sums: grad_a = " +
        	w.sum_grad_a + "; grad_f = " + w.sum_grad_f + "; grad_g = " + w.sum_grad_g + "; grad_h = " + w.sum_grad_h +
        	"; grad_s = " + w.sum_grad_s +  "; grad_b = " + w.sum_grad_b +
        	";  Micro F1 for 1K Validation set = " + f1 + "\n"
      println(str)        	
      (str,f1, reg, eta, adaGrad)  
    }}.sortBy(_._2).reverse

    println("\n\nRESULTS OF FINDING SGD Parameters on the validation set: #################### ")
    results.foreach(f => println(f._1))    
  }

  
  /*
   * Find best parameters to run SGD.
   */
  def findSGDParametersAndStartSGD(NUM_ITERS : Int, max_product : Boolean, sc : SparkContext) = {
    println("\n\n\n=========================================")
    println("====== Start grid search to find best reg and eta ==================")
    println("===============================================")
    println("\n\n\n")
    
    val regularizers = List(0.00001, 0.001, 0.01, 0.0)
    val etas = List(0.01, 0.1, 1.0, 5.0)
    val adaGrads = List(true) //, false)
        
    val paramsList = for (reg <- regularizers; eta <- etas; adaGrad <- adaGrads) yield (reg,eta, adaGrad)
      
    val oneShardData = splittedTrainingDataRDD.take(1)(0)
    val trainingDataBroadcast = sc.broadcast(oneShardData)
    
    /*
     * Train on a sample of Wikipedia
     */
    val params = sc.parallelize(paramsList, paramsList.size)
      .map{ case (reg,eta, adaGrad) => {
        val data = trainingDataBroadcast.value
        println("   TrainingDataSize : " + data.size)
        var w = new ScorerWeights
        for (iter <- 1 to ((NUM_ITERS / data.length) + 1)) {        
          w = SingleThreadSGD(w, iter, data, eta, reg, Math.min(NUM_ITERS, data.length), adaGrad)
        }        
        (w, reg, eta, adaGrad)
      }}.collect
    
    val results = params.map{case (w, reg, eta, adaGrad) => {
      val f1 = EvalOnDatasets.evalAllDatasets(
          false,
          validationData, w, null,
          "Validation set for reg = " + reg + " eta = " + eta + " ; adaGrad = " + adaGrad, 
          fullPrint = false, max_product, sc)
          
      val str = "Done loopy training for reg = " + reg + " eta = " + eta + " ; adaGrad = " + adaGrad +
        	"; Weights a = " + w.a + "; f = " + w.f + "; g = " + w.g + "; h = " + w.h + "; s = " + w.s + "; b = " + w.b +
        	" adagrad iters = " + w.num_grads +
        	";  Micro+Macro F1 for 1K Validation set = " + f1._1 + "\n\n\n\n\n\n"
      println(str)        	
      (str,f1, reg, eta, adaGrad)  
    }}.sortBy(_._2).reverse

    println("\n\nRESULTS OF FINDING SGD Parameters on the validation set: #################### ")
    results.foreach(f => println(f._1))    

    /*
     * Start actual SGD using the best params
     */
    val regularizer = results(0)._3
    val eta = results(0)._4 
    val NUM_ROUNDS = 1000
    val NUM_ITERS_PER_ROUND_PER_WORKER = 5000 // 5% of all data for one worker
    val adaGrad = results(0)._5 

    println("\n\n\n\n\n\n\n\n\n***********************************************************")
    println("\n\n******** Start SGD with : eta = " + eta + " reg = " + regularizer + " adaGrad : " + adaGrad + " **********\n\n")
    SGD(eta, regularizer, NUM_ROUNDS, NUM_WORKERS, NUM_ITERS_PER_ROUND_PER_WORKER, max_product, adaGrad, sc)    
  }
  
  
  
  /* 
   * Used to discover the best values of regularizer.
   */
  def SingleThreadSGD(
      w : ScorerWeights,
      numRound : Int,
      docs : Array[(Int, Array[(String, Int, Array[Int])])],
      eta : Double, 
      regularizer : Double, 
      numIters : Int,
      adaGrad : Boolean) : ScorerWeights = {
    
    println("\nStart Single Thread SGD Few params with regularizer = " + regularizer +
        " ; eta = " + eta + " adaGrad : " + adaGrad + "; num iters = " + numIters + " ; num docs = " + docs.size )
    
    val mergedW = LocalSGD(w.clone, docs, eta, regularizer, numIters, numRound, adaGrad) 
      
    println
    mergedW.print
    println("\n======== Done SGD  with regularizer = " + regularizer +
        " ; eta = " + eta  + " adaGrad : " + adaGrad + " ================\n\n\n\n")
    return mergedW
  }  
 
  /* Local AdaGrad SGD in rounds for Piecewise or Pseudolikelihood learning:
   * - perform a number of rounds
   *    - in each round split the data on different nodes
   *    - each node does a full SGD on its partition of data
   *    - at the end of the round, all nodes average their results and the sums of historical gradients
   *    - the new averaged weight is passed to the next round to all nodes
   * - final weight vector is reported
   */   
  def SGD(
      eta: Double, 
      regularizer : Double, 
      numSGDRounds : Int,
      numSGDWorkers : Int,
      numItersPerRound : Int,
      max_product : Boolean,
      adaGrad : Boolean,
      sc : SparkContext) = {
    
    println()
    println("\n\n\n=======================================================")
    println("Start SGD Few params with eta = " + eta + "; regularizer = " + regularizer + 
        " adaGrad : " + adaGrad + "; num workers = " + numSGDWorkers + "; num rounds = " +
        numSGDRounds + "; num iters per round per worker = " + numItersPerRound)
    
    var w = new ScorerWeights

    var oldW = w.clone
    
    for (round <- 1 to numSGDRounds) {
      println("\n\n ************* Start round " + round + " ***********\n\n")
      
      val mergedW = splittedTrainingDataRDD
        .map(localTrainingData =>
          LocalSGD(w.clone, localTrainingData, eta, regularizer, numItersPerRound, round, adaGrad) 
        )
        .reduce((x,y) => x.add(y))
      
      w = mergedW.multiply(1.0 / numSGDWorkers)
      println
      println("Finished SGD round = " + round)
      w.print
      println("Delta squared gradients after round " + round + 
          " delta_a = " + (w.squares_grad_a - oldW.squares_grad_a) +
          " delta_f = " + (w.squares_grad_f - oldW.squares_grad_f) +
          " delta_g = " + (w.squares_grad_g - oldW.squares_grad_g) +
          " delta_h = " + (w.squares_grad_h - oldW.squares_grad_h) +
          " delta_s = " + (w.squares_grad_s - oldW.squares_grad_s) +
          " delta_b = " + (w.squares_grad_b - oldW.squares_grad_b))
      println("Delta sum gradients after round " + round +
          " delta sum_a = " + (w.sum_grad_a - oldW.sum_grad_a) +
          " delta sum_f = " + (w.sum_grad_a - oldW.sum_grad_f) +
          " delta sum_g = " + (w.sum_grad_g - oldW.sum_grad_g) +
          " delta sum_h = " + (w.sum_grad_h - oldW.sum_grad_h) +
          " delta sum_s = " + (w.sum_grad_s - oldW.sum_grad_s) + 
          " delta sum_b = " + (w.sum_grad_b - oldW.sum_grad_b))
      println
      oldW = w.clone
      
      // Do testing after a chunck of iterations
      if (round % 3 == 0) {
        println("\n\nStart testing at master ")
        w.print
        EvalOnDatasets.evalAllDatasets(false, testData, w, null, "Loopy Few params SGD after " + round + " rounds", false, max_product, sc)
        w.print
      }  
    }

    println("\n\n\n======== Done SGD ================\n\n\n\n")
    w.print
    EvalOnDatasets.evalAllDatasets(false, testData, w, null, "Loopy Few params SGD after SGD done. ", false, max_product, sc)
  }
  
 /*
  * Local AdaGrad SGD on a single machine.
  */    
  def LocalSGD(
      w : ScorerWeights,
      localTrainingData : Array[(Int, Array[(String, Int, Array[Int])])],
      eta : Double, 
      regularizer : Double, 
      numIters : Long,
      numRound : Int,
      adaGrad : Boolean) 
  : ScorerWeights = {

    // Random shuffle the data.
    var list = (0 until localTrainingData.length).toList
    list = util.Random.shuffle(list)
    
    val trainingPtsForTesting =  localTrainingData.take(50)
    var oldW = w.clone

    val workerId = (Math.random() * 10000).toInt
    println("Start local SGD for worker " + workerId + " ;eta = " + eta + 
        "; regularizer = " + regularizer + " adaGrad : " + adaGrad )

    var i : Int = 0
    while (i < numIters) {
      i += 1
      
      val randomDoc = localTrainingData(list(i % localTrainingData.length))

      // Speed-up this training by splitting long documents into small docs with len <= 25.
      val inputPairsListIter = randomDoc._2.grouped(25)
      
      while (inputPairsListIter.hasNext) {
        SGDStepPseudolikelihood(w, (numRound - 1) * numIters + i + 1, eta, regularizer, inputPairsListIter.next, adaGrad)      
      }

      if (i%100 == 0){
        println("\nWorker = " + workerId + "; Start iter i = " + i + " ;eta = " + eta + 
        "; regularizer = " + regularizer + " adaGrad : " + adaGrad + 
        " ; loss on 50 training pts = " + lossAndDerivates(w, trainingPtsForTesting, regularizer)._1) 
        w.print        
        println("=== Delta squared gradients " + " ;eta = " + eta + 
          "; regularizer = " + regularizer + " adaGrad : " + adaGrad +
          ": delta_a = " + (w.squares_grad_a - oldW.squares_grad_a) +
          ": delta_f = " + (w.squares_grad_f - oldW.squares_grad_f) +
          " delta_g = " + (w.squares_grad_g - oldW.squares_grad_g) +
          " delta_h = " + (w.squares_grad_h - oldW.squares_grad_h) + 
          " delta_s = " + (w.squares_grad_s - oldW.squares_grad_s) +
          " delta_b = " + (w.squares_grad_b - oldW.squares_grad_b))
          
        oldW = w.clone        
        println
      }
    }

    println("Finished round " + numRound + " of local SGD for worker = " + workerId + " after iterations num = " + i + "\n")
    w
  }


  /*
   * One SGD update step for a single document using Pseudolikelihood learning. 
   */
  def SGDStepPseudolikelihood(
      w : ScorerWeights,
      numPoint : Long,
      eta : Double,
      regularizer : Double, 
      inputPairs : Array[(String,Int, Array[Int])],
      adaGrad : Boolean) : Boolean =  {
    
    val loss_evals = lossAndDerivates(w, Array((1, inputPairs)), regularizer)
    
    if (!adaGrad) {
      w.a += eta / (numPoint + 1000) * loss_evals._2.get("a")
      w.f += eta / (numPoint + 1000) * loss_evals._2.get("f")
      w.g += eta / (numPoint + 1000) * loss_evals._2.get("g")
      w.h += eta / (numPoint + 1000) * loss_evals._2.get("h")
      w.s += eta / (numPoint + 1000) * loss_evals._2.get("s")
      w.b += eta / (numPoint + 1000) * loss_evals._2.get("b")
    } else {
      w.num_grads += 1
      w.squares_grad_a += loss_evals._2.get("a") * loss_evals._2.get("a")
      w.squares_grad_f += loss_evals._2.get("f") * loss_evals._2.get("f")
      w.squares_grad_g += loss_evals._2.get("g") * loss_evals._2.get("g")
      w.squares_grad_h += loss_evals._2.get("h") * loss_evals._2.get("h")
      w.squares_grad_s += loss_evals._2.get("s") * loss_evals._2.get("s")
      w.squares_grad_b += loss_evals._2.get("b") * loss_evals._2.get("b")
      w.sum_grad_a += loss_evals._2.get("a")
      w.sum_grad_f += loss_evals._2.get("f")
      w.sum_grad_g += loss_evals._2.get("g")
      w.sum_grad_h += loss_evals._2.get("h")
      w.sum_grad_s += loss_evals._2.get("s")
      w.sum_grad_b += loss_evals._2.get("b")
      
      // Add a +1 under the sqrt, like here: http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40808.pdf
      val etaAdaGrad_a = eta / Math.sqrt(1.0 + w.squares_grad_a)
      val etaAdaGrad_f = eta / Math.sqrt(1.0 + w.squares_grad_f)
      val etaAdaGrad_g = eta / Math.sqrt(1.0 + w.squares_grad_g)
      val etaAdaGrad_h = eta / Math.sqrt(1.0 + w.squares_grad_h)
      val etaAdaGrad_s = eta / Math.sqrt(1.0 + w.squares_grad_s)
      val etaAdaGrad_b = eta / Math.sqrt(1.0 + w.squares_grad_b)
      w.a += etaAdaGrad_a * loss_evals._2.get("a")
      w.f += etaAdaGrad_f * loss_evals._2.get("f")
      w.g += etaAdaGrad_g * loss_evals._2.get("g")
      w.h += etaAdaGrad_h * loss_evals._2.get("h")      
      w.s += etaAdaGrad_s * loss_evals._2.get("s")      
      w.b += etaAdaGrad_b * loss_evals._2.get("b")      
    }
    
    w.projectWBackInConvexSet
    
    true
  }
}