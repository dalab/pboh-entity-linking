package eval.cweb

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import index.AllIndexesBox
import loopybeliefpropagation.ScorerFull
import loopybeliefpropagation.ScorerWeights
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import gnu.trove.set.hash.THashSet
import eval.Annotation
import scala.collection.JavaConversions._
import gnu.trove.map.hash.TObjectIntHashMap
import scala.collection.mutable.ArrayBuffer
import eval.GlobalStats
import eval.VerifyEDLBPForListOfWords
import eval.Verifier
import utils.Normalizer

/*
 * 
 * To Run:
 * MASTER=spark://dco-node040.dco.ethz.ch:7077 ~/spark/bin/spark-submit
 *  --class "EL_LBP_Spark"  --executor-memory 100G --total-executor-cores 128
 *   --jars lib/el_lbp_clueweb_list_words_2.10-1.0.jar lib/el-lbp.jar 
 *   runLoopyAgainstAllGoogleClweb
 *   /mnt/cw12/other-data/FreebaseAnnotationsOfClueweb12/ClueWeb12_00/ |
 *    tee results-marina-27oct-google-loopy-cweb
 */ 

// Singleton wrapper for all indexes. If you try to load it separately in each
// mapPartion you will fail miserably. This singleton means that the indexes
// will be loaded just once per node, at the initialization.
// http://apache-spark-user-list.1001560.n3.nabble.com/How-to-share-a-NonSerializable-variable-among-tasks-in-the-same-worker-node-td11048.html#a11315
object AllIndexesWrapper {
  private val indexesToLoad =
    "mentionEntsFreqIndex,entIDToNameIndex,entNameToIDIndex, redirectIndex, wikipEntityCooccurrIndex, freebaseToWikiIDIndex"

  private val allIndexes = new AllIndexesBox(indexesToLoad)    
    
  def getAllIndexesBox : AllIndexesBox = allIndexes
}

object RunLoopyAgainstAllGoogleClweb {
  
  // Debug info:
  def printDiffsLoopyArgmax(
      verifierLoopy : Verifier[Annotation],
      verifierARGMAX : Verifier[Annotation],
      tkspans: Array[String], 
      entities : Array[Int],
      offsets : Array[Int],
      allIndexesBox : AllIndexesBox,
      scorer : ScorerFull) : Boolean = {

    if (tkspans.size > 50) {
      return false
    }
    
    val googleSet = new TObjectIntHashMap[String]
    for (i <- 0 until entities.size) {
      googleSet.put(tkspans(i) + "\t" + offsets(i).toInt, entities(i).toInt)
    }
    
    var loopyAnnots = verifierLoopy.getCorrectAnnotations()
    loopyAnnots.addAll(verifierLoopy.getWrongAnnotations())
    val loopyIter = loopyAnnots.iterator()
    val loopySet = new THashSet[Annotation]
    while (loopyIter.hasNext()) {
      loopySet.add(loopyIter.next())
    }
    
    var argmaxAnnots = verifierARGMAX.getCorrectAnnotations()
    argmaxAnnots.addAll(verifierARGMAX.getWrongAnnotations())
    val argmaxIter = argmaxAnnots.iterator()
    val argmaxSet = new THashSet[Annotation]
    while (argmaxIter.hasNext()) {
      argmaxSet.add(argmaxIter.next())
    }
    
    println("\n***************************")
    println("Loopy recall : " + verifierLoopy.getRecall())
    println("Argmax recall : " + verifierARGMAX.getRecall())
    println("Intersection of results:")
    for (la <- loopySet.iterator()) {
      if (argmaxSet.contains(la)) {
        val googleEnt = googleSet.get(la.getMention.getNgram + "\t" + la.getMention.getOffset)
        if (la.getEntity == googleEnt) {
          println("Tksp: " + la.getMention.getNgram+
                " ;LoopyArgmaxGoogle: " +
                allIndexesBox.entIDToNameIndex.get(la.getEntity))              
        } else {
          println("Tksp: " + la.getMention.getNgram +
                " ;LoopyArgmax: " +
                allIndexesBox.entIDToNameIndex.get(la.getEntity) + 
                " ;Google: "  + allIndexesBox.entIDToNameIndex.get(googleEnt))            
        }
      }
    }
    
    println("Differences of results:")
    for (la <- loopySet) {
      if (!argmaxSet.contains(la)) {
        var found = false
        for (aa <- argmaxSet) {
          if (la.getMention.getOffset == aa.getMention.getOffset && 
              la.getMention.getLength == aa.getMention.getLength &&
              la.getMention.getNgram == aa.getMention.getNgram) {
            found = true
            
            val googleEnt = googleSet.get(la.getMention.getNgram + "\t" + la.getMention.getOffset)
            println("Tksp: " + la.getMention.getNgram +
                " ;Loopy: " + allIndexesBox.entIDToNameIndex.get(la.getEntity) +
                " ;Argmax: " + allIndexesBox.entIDToNameIndex.get(aa.getEntity)+ 
                " ;Google: "  + allIndexesBox.entIDToNameIndex.get(googleEnt))
            
            // Print scores for entities inside the intersection.
            if (scorer != null) {
              for (commona <- loopySet) {
                if (argmaxSet.contains(commona)) {
                  val commonEnt = commona.getEntity
                  scorer.printScorerParams("Loopy", la.getEntity, la.getMention.getNgram, commonEnt)
                  scorer.printScorerParams("Argmax", aa.getEntity, aa.getMention.getNgram, commonEnt)
                  println
                }
              }
            }
          }
        }
        if (!found) {
          println("NOT FOUND ARGMAX IN LOOPY ORIGINAL!!")
        }
      }
    }
    println("***************************")
    return true
  }

  
  def solveOneSmallFile(
      smallFile: (String, Iterable[String]), 
      allIndexesBox : AllIndexesBox,
      priorConstant : Double,
      printDebugInfo : Boolean,
      max_product : Boolean)
  : (GlobalStats, GlobalCountersCWEB) = {
    
    val localCounters = new GlobalCountersCWEB
    var globalStats = new GlobalStats

    var tkspans = new ArrayBuffer[String]
    var entities = new ArrayBuffer[Int]
    var offsets = new ArrayBuffer[Int]
    
    // Prepare vector of ground truth mentions and entities.
    for (line <- smallFile._2) {
      val elements = line.split("\t")
      if (elements.size != 8) {
        throw new Exception("Clueweb annotation line is not well formated: " + line)
      }
      val freebaseId = elements(7).substring(elements(7).indexOf("/m/") + "/m/".length())
      val mention = Normalizer.normalizeLowercase(elements(2))

      ////// DEBUG: Update global counters just for unseen mentions.
      if (!tkspans.contains(mention)) {
        localCounters.update(mention, freebaseId, allIndexesBox)
      }

      /*
       * Keep just ground truth mentions for which we have a corresponding
       * Wikipedia entity.
       * 
       * No need to remove mentions that are not in our mentionFreq index,
       * because these will be filtered in the Loopy algorithm.
       * 
       * Also, keep just distinct mentions (no duplicates).
       */
      if (allIndexesBox.freebaseToWikiIDIndex.containsKey(freebaseId) &&
          !tkspans.contains(mention)) {
        tkspans += mention
        offsets += elements(3).toInt
        entities += allIndexesBox.freebaseToWikiIDIndex.get(freebaseId)
        }
      }
    
      println("\nNum mentions to analyse = " + tkspans.size)

      if (tkspans.size() < 800)  { //HACK to make it take less time
        // Run EL-LBP and ARGMAX:
        var stats = new GlobalStats
        val scorerWeights = new ScorerWeights(priorConstant,1,1,1,1,1)
        val entityLinkingRunner = new VerifyEDLBPForListOfWords(allIndexesBox, tkspans.toArray, entities.toArray, null, offsets.toArray, scorerWeights)
      
        if (tkspans.size() > 0) {
          // Process and run the EL-LBP algorithm.
          val lbpresults = entityLinkingRunner
          		.runEDLoopyOnOneInputList(max_product)
          
          val verifierLoopy = lbpresults.verifier
          
          // Process and run ARGMAX version.
          val verifierARGMAX = 
            entityLinkingRunner.runEDArgmaxOnOneInputList()
          
          // DEBUG:        
          if (printDebugInfo && verifierLoopy.getRecall() < verifierARGMAX.getRecall()) {
            printDiffsLoopyArgmax(verifierLoopy, verifierARGMAX, 
                tkspans.toArray, entities.toArray, offsets.toArray, allIndexesBox, null)
          }
            
          // Gather statistics
          stats.addOneDoc(verifierLoopy, verifierARGMAX, lbpresults.lbpstats);
        }
        globalStats = globalStats.add(stats)
      }
    
    (globalStats, localCounters)
  }
  
  
  def run(args: Array[String], priorConstant : Double, max_product : Boolean) {
    println("\nProcessing directory:" + args(0))

    val conf = new SparkConf().setAppName("EL LBP SPARK")
    conf.set("spark.cores.max", "128")
    conf.set("spark.akka.frameSize", "1024")
    conf.set("spark.executor.memory", "100g")
    conf.set("spark.shuffle.file.buffer.kb", "1000")
    
    val sc = new SparkContext(conf)

    val globalCounters = new GlobalCountersCWEB

    var finalStats = new GlobalStats
    
    var numBigFiles = 0
    val root = new java.io.File( args(0) )
    val listOfDirs = root.listFiles()
    for ( dir <- listOfDirs ) {
      val listOfFiles = (new java.io.File(dir.getAbsolutePath())).listFiles()
      for (f <- listOfFiles) {
        numBigFiles += 1
        println("=====================================================================")
        println("Analyzing file : " + f.getAbsolutePath())
        
        val partialStats = sc.textFile(f.getAbsolutePath())
        	.map(line => { val smallFileName = line.split("\t").head; (smallFileName , line)} )
        	.groupByKey(256)  // Group by smallFileName
        	.map{ 
        		smallFile =>
        		  solveOneSmallFile(
        		      smallFile, 
        		      AllIndexesWrapper.getAllIndexesBox,
        		      priorConstant,
        		      true,
        		      max_product) 
        	}
        	.reduce((a,b) => (a._1.add(b._1), {val x = new GlobalCountersCWEB; x += a._2;  x += b._2; x}))

        globalCounters += partialStats._2
        finalStats = finalStats.add(partialStats._1)
        println("=== Partial results after analysing " + numBigFiles + " big files ===")
        println("=====================================================================")
        
        globalCounters.printAll        
        finalStats.print(true)
      }
    }
  
    
    println("=====================================================================")
    println("============================ Finished ===============================")
    println("=====================================================================")
    sc.stop()
  }
}
