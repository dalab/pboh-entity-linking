package eval

import scala.collection.mutable.MutableList
import java.util.ArrayList
import index.AllIndexesBox
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import loopybeliefpropagation.LBPPair
import loopybeliefpropagation.ScorerWeights
import eval.datasets.AQUAINT_MSNBC_ACE04
import utils.OptimizedLambdasMap
import utils.OptimizedRhosMap
import gnu.trove.map.hash.THashMap
import eval.datasets.AIDA
import eval.datasets.WikipediaDataset
import eval.datasets.GERBIL_MSNBC

object AllIndexesForEval {
  private val indexesToLoad =
    "mentionEntsFreqIndex,entIDToNameIndex,entNameToIDIndex,redirectIndex,wikipEntityCooccurrIndex,wordFreqDict,wordEntityProbs"
  
  private val allIndexes = new AllIndexesBox(indexesToLoad)
  
  def getAllIndexesBox : AllIndexesBox = allIndexes
}

object AllIndexesForParsingEval {
  private val indexesToLoad =
    "wordFreqDict, entNameToIDIndex, redirectIndex"
  private val allIndexes = new AllIndexesBox(indexesToLoad)
  
  def getAllIndexesBox : AllIndexesBox = allIndexes
}

object EvalOnDatasets {
  
  def evalWikiValidationData(
      wikipediaValidationData: Array[(Int, Array[(String, Int, Array[Int])])],
      fewWeights : ScorerWeights,
      w : (OptimizedRhosMap, OptimizedLambdasMap),
      banner : String,
      fullPrint : Boolean,
      max_product : Boolean,
      sc : SparkContext) : (Double, Double, Double) = {    
    val paramsBannerNew = " params a = " + fewWeights.a + ", f = " + fewWeights.f +  ", g = " + fewWeights.g +
    	", h = " + fewWeights.h + ", s = " + fewWeights.s + ", b = " + fewWeights.b
    
    evalOneDatasetInParallel(wikipediaValidationData.map(x => ("" + x._1, x._2)), 
        fewWeights, w, banner + paramsBannerNew, " Wikipedia 1K validation set ", fullPrint, max_product, sc)

    null
  }
  
  
  def evalAllDatasets(
      allSets : Boolean,
      wikipediaTestData: Array[(Int, Array[(String, Int, Array[Int])])],
      fewWeights : ScorerWeights,      
      w : (OptimizedRhosMap, OptimizedLambdasMap),
      banner : String,
      fullPrint : Boolean,
      max_product : Boolean,
      sc : SparkContext) : (Double, Double) = {    

    val allIndexesBox = AllIndexesForParsingEval.getAllIndexesBox
    
    val paramsBannerNew = " params a = " + fewWeights.a + ", f = " + fewWeights.f +  ", g = " + fewWeights.g + 
    	", h = " + fewWeights.h + ", s = " + fewWeights.s + ", b = " + fewWeights.b

      evalOneDatasetInParallel(AIDA.loadDataset(true, allIndexesBox),  
        fewWeights, w, banner + paramsBannerNew, " AIDA test A", fullPrint, max_product, sc)
    
    return null ///////////////// DELETE
    
    val stats_AIDA_A = 
      evalOneDatasetInParallel(AIDA.loadDataset(true, allIndexesBox),  
        fewWeights, w, banner + paramsBannerNew, " AIDA test A", fullPrint, max_product, sc)
  
    val stats_Wiki = 
      evalOneDatasetInParallel(WikipediaDataset.loadDataset(allIndexesBox),  
        fewWeights, w, banner + paramsBannerNew, " Wikipedia VALIDATION ", fullPrint, max_product, sc)    	
        
    if (allSets) {
      evalOneDatasetInParallel(AIDA.loadDataset(false, allIndexesBox),  
          fewWeights, w, banner + paramsBannerNew, " AIDA test B", fullPrint, max_product, sc)
      evalOneDatasetInParallel(AQUAINT_MSNBC_ACE04.loadDataset("MSNBC", allIndexesBox),  
          fewWeights, w, banner + paramsBannerNew, " MSNBC ", fullPrint, max_product, sc) 
      evalOneDatasetInParallel(AQUAINT_MSNBC_ACE04.loadDataset("AQUAINT", allIndexesBox),  
          fewWeights, w, banner + paramsBannerNew, " AQUAINT ", fullPrint, max_product, sc) 
      evalOneDatasetInParallel(AQUAINT_MSNBC_ACE04.loadDataset("ACE04", allIndexesBox),  
          fewWeights, w, banner + paramsBannerNew, " ACE04 ", fullPrint, max_product, sc)
    }
        
    val combinedMicro = (stats_Wiki._1 * stats_Wiki._3 + stats_AIDA_A._1 * stats_AIDA_A._3) / (stats_Wiki._3 + stats_AIDA_A._3)
    val combinedMacro = (stats_Wiki._2 * stats_Wiki._4 + stats_AIDA_A._2 * stats_AIDA_A._4) / (stats_Wiki._4 + stats_AIDA_A._4)
        
    return (stats_AIDA_A._1 + stats_AIDA_A._2, 
        combinedMicro + combinedMacro)

        
    evalOneDatasetInParallel(wikipediaTestData.map(x => ("" + x._1, x._2)),  
        fewWeights, w, banner + paramsBannerNew, " Wikipedia 1K random pages ", fullPrint, max_product, sc)
    null
  }

  /*
   * Returns global micro accuracy (per mention).
   */
  def evalOneDatasetInParallel(
      testData: Array[(String, Array[(String, Int, Array[Int])])],
      fewWeights : ScorerWeights,      
      w : (OptimizedRhosMap, OptimizedLambdasMap),
      banner : String,
      datasetTitle : String,
      fullPrint : Boolean,
      max_product : Boolean,
      sc : SparkContext) : (Double, Double, Int, Int) = {    
    
    var total_num_mentions = 0
    
    val testDataSize = testData.size

    for (j <- 0 until testDataSize) {
      val docId = testData(j)._1 
      val input = testData(j)._2
      total_num_mentions += input.size
    }
    
    val stats = testData
    		.map(doc => evalOneDoc(doc, fewWeights, w, max_product))
    		.reduce((x,y) => x.add(y))
    
    println ("\n ############### RESULTS for dataset " + datasetTitle + " for \n " + banner + " ################# ")
    println("Num total docs = " + testDataSize)
    println("Num total mentions (including duplicates) = " + total_num_mentions)
    stats.print(fullPrint)
    println("==============================================\n")
    
    return (stats.microF1, stats.macroF1, stats.numGroundTruthMention(), stats.numDocs)
  }
  
  def evalOneDoc(
      doc : (String, Array[(String, Int, Array[Int])]),
      fewWeights : ScorerWeights,
      w : (OptimizedRhosMap, OptimizedLambdasMap),
      max_product : Boolean) : GlobalStats = {
    
    var localStats = new GlobalStats

    val allIndexesBox = AllIndexesForEval.getAllIndexesBox
    val docId = doc._1 
    val input = doc._2

    var tkspans = new ArrayBuffer[String]
    var entities = new ArrayBuffer[Int]
    var contexts = new THashMap[String, Array[Int]]
    var offsets = new ArrayBuffer[Int]

    // Prepare vector of ground truth mentions and entities.
    for (i <- 0 until input.size) {
      val mention = input(i)._1.toLowerCase().trim()
      val ent = input(i)._2
      val context = input(i)._3 

      // Keep all mentions (including duplicates).
      tkspans += mention
      offsets += i
      entities += ent
      contexts.put(mention, context)
      
    }

    if (tkspans.size < 1000)  { //HACK to make it take less time
      // Run EL-LBP and ARGMAX:
      val entityLinkingRunner = new VerifyEDLBPForListOfWords(allIndexesBox, tkspans.toArray,entities.toArray, contexts, offsets.toArray, fewWeights)
      
      if (tkspans.size > 0) {
        // Process and run the EL-LBP algorithm.
        var lbpresults : LBPPair = null
        
        if (w != null) {
          lbpresults = entityLinkingRunner
        		  .runEDLoopyFullLambdasAndRhosLearnedOnOneInputList(
        		      tkspans.toArray, entities.toArray, offsets.toArray, w._1, w._2, fewWeights, max_product)
        } else {
          lbpresults = 
            entityLinkingRunner.runEDLoopyOnOneInputList(max_product)
        }
        
        val verifierLoopy = lbpresults.verifier
        val lbpRunningStats = lbpresults.lbpstats
        
        // Process and run ARGMAX version.
        val verifierARGMAX = 
          entityLinkingRunner.runEDArgmaxOnOneInputList()
        localStats.addOneDoc(verifierLoopy, verifierARGMAX, lbpRunningStats)
      }
    }

    localStats    
  }
  
}