package eval

import index.AllIndexesBox
import gnu.trove.set.hash.THashSet
import gnu.trove.map.hash.THashMap
import loopybeliefpropagation.ScorerWeights
import loopybeliefpropagation.ScorerFull
import md.MostFrequentEntity
import scala.collection.JavaConversions._
import loopybeliefpropagation.LBPPair
import loopybeliefpropagation.LoopyBeliefPropagation
import utils.OptimizedRhosMap
import utils.OptimizedLambdasMap
import loopybeliefpropagation.ScorerFullLearnedParams
import md.Mention
import index.MentEntsFreqIndexWrapper
import loopybeliefpropagation.RhoCache

class VerifyEDLBPForListOfWords(
    allIndexesBox : AllIndexesBox, 
    tkspans : Array[String], // can be uppercase !!!!
    entities : Array[Int],
    contexts : THashMap[String, Array[Int]],      
    offsets : Array[Int],
    scorerWeights : ScorerWeights) extends VerifyEDAbstract(allIndexesBox) {
  
  val rhoCache = new RhoCache
  val mentEntsCache = new MentEntsFreqIndexWrapper(allIndexesBox, rhoCache, tkspans, contexts, scorerWeights)
  val groundTruthAnnotations = 
      convertListOfTkspansAndEntitiesIntoSetOfAnnotations(tkspans, entities, offsets)
  val analyzedMentions = getMentions(groundTruthAnnotations, mentEntsCache)
  val scorer = new ScorerFull(analyzedMentions, contexts, allIndexesBox, rhoCache, mentEntsCache, scorerWeights)
  
  
  val iterations = 15

  private def convertListOfTkspansAndEntitiesIntoSetOfAnnotations(
      tkspans : Array[String], 
      entities : Array[Int], 
      offsets : Array[Int]) : THashSet[Annotation] = {
    
    assert(entities.size == tkspans.size && entities.size == offsets.size,
        "[FATAL] Input vectors entities, offsets and token spans have different sizes.")
	
	val annotations = new THashSet[Annotation]()
	for (i <- 0 until entities.size) {
	  
	  annotations.add(
	      new Annotation(entities(i), 1.0, new Mention(tkspans(i), offsets(i)), "", entities(i)));
	}
    return annotations;
  }

  
  def runEDArgmaxOnOneInputList() : Verifier[Annotation] = {
 
    val entityDisambiguator = new MostFrequentEntity(mentEntsCache, scorer)
    val solutionMap = entityDisambiguator.solve(analyzedMentions)
    val solAnnotations = new THashSet[Annotation]()
    
    for (e <- solutionMap.entrySet()) {
      val m = e.getKey()
      val entity = e.getValue().entity 
      val score = e.getValue().score 
      solAnnotations.add(
          new Annotation(entity, score, m, "", m.getGroundTruthEntity))
    }
    
    val verifier = verifyAnnotations("ARGMAX", solAnnotations, groundTruthAnnotations, null, false)
    return verifier
  }	
	
  
  def runEDLoopyOnOneInputList(max_product : Boolean) : LBPPair = {
    
    val filePath = ""
    val lbp = new LoopyBeliefPropagation(analyzedMentions, iterations, scorer, allIndexesBox, mentEntsCache)
    val lbpstats = lbp.solve(max_product)
    val solution = lbp.getSolutionAnnotations(filePath)
    
    val verifier = verifyAnnotations("Loopy", solution, groundTruthAnnotations, scorer, true)
    return new LBPPair(verifier, lbpstats)
  }

  
//////////////////////////////////////////////////////////////////////////////////////////////////////////////  
  def runEDLoopyFullLambdasAndRhosLearnedOnOneInputList(
      tkspans : Array[String],
      entities : Array[Int],
      offsets : Array[Int],
      rhos : OptimizedRhosMap,
      lambdas : OptimizedLambdasMap,
      scorerWeights : ScorerWeights,
      max_product : Boolean) : LBPPair = {

    val filePath = ""
    val scorer2 = new ScorerFullLearnedParams(analyzedMentions, rhos, lambdas, allIndexesBox, mentEntsCache, scorerWeights)
    
    val lbp = new LoopyBeliefPropagation(analyzedMentions, iterations, scorer2, allIndexesBox, mentEntsCache)
    val lbpstats = lbp.solve(max_product)
    val solution = lbp.getSolutionAnnotations(filePath)
    
    val verifier = verifyAnnotations("Loopy", solution, groundTruthAnnotations, null, false)
    return new LBPPair(verifier, lbpstats)
  }  
  
  // For debug only:
  def getLoopyScorerForOneInputList(
      tkspans : Array[String],
      entities : Array[Int],
      contexts : THashMap[String, Array[Int]],      
      offsets : Array[Int],
      scorerWeights : ScorerWeights) : ScorerFull = {

    val groundTruthAnnotations =
      convertListOfTkspansAndEntitiesIntoSetOfAnnotations(tkspans, entities, offsets);
    val mentEntsCache = new MentEntsFreqIndexWrapper(allIndexesBox, new RhoCache, tkspans, contexts, scorerWeights)
    val analyzedMentions = getMentions(groundTruthAnnotations, mentEntsCache)
    val scorer = new ScorerFull(analyzedMentions, contexts, allIndexesBox, new RhoCache, mentEntsCache, scorerWeights);
    return scorer;
  }	
}
