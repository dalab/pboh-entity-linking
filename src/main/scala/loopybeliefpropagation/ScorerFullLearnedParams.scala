package loopybeliefpropagation;

import index.AllIndexesBox
import md.Mention
import utils.OptimizedRhosMap
import utils.OptimizedLambdasMap
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.THashMap
import scala.collection.JavaConversions._
import index.MentEntsFreqIndexWrapper


class ScorerFullLearnedParams(
    mentions : Array[Mention], 
    rhos : OptimizedRhosMap, 
    lambdas : OptimizedLambdasMap,
    allIndexesBox : AllIndexesBox,
    mentEntsCache : MentEntsFreqIndexWrapper) extends Scorer {
	
  private var defaultScorer : ScorerFull = null;

  def this(
      mentions : Array[Mention],
      rhos : OptimizedRhosMap, 
      lambdas : OptimizedLambdasMap,
      allIndexesBox : AllIndexesBox,
      mentEntsCache : MentEntsFreqIndexWrapper,
      scorerWeights : ScorerWeights) = {
    this(mentions, rhos, lambdas, allIndexesBox, mentEntsCache)
    this.defaultScorer = new ScorerFull(mentions, null, allIndexesBox, new RhoCache, mentEntsCache, scorerWeights)
  }

  def computeMessageScore(
      from : Mention, to : Mention, 
      entityFrom : Int, entityTo : Int, oldMessages : MessagesMap) : Double = {
    
    val l = mentions.size
    var rho = 0.0
    
    if (rhos.contains(from.getNgram(), entityFrom)) {
      rho = rhos.getRho(from.getNgram(), entityFrom);
    } else {
      rho = defaultScorer.rho(from.getNgram(), entityFrom, false);
    }

    var lambda = 0.0;
    if (lambdas.contains(entityFrom, entityTo)) {
      lambda = 2.0  / (l * (l - 1)) * lambdas.getOrElseZero(entityFrom, entityTo);
    } else {
      lambda = defaultScorer.scorerWeights.g * 2.0  / (l * (l - 1)) * defaultScorer.lambda(entityFrom, entityTo, false);
    }
	
    val messagesNeighbors = oldMessages.sumNeighborMessages(from, entityFrom, to, mentions);
    return messagesNeighbors + rho + lambda;
  }
      
  def computeScores(messages: MessagesMap) : TObjectDoubleHashMap[(Mention, Int)] = {
    val result = new TObjectDoubleHashMap[(Mention, Int)]();
    for (mention <- mentions) {
      for (entity <- mentEntsCache.getCandidateEntities(mention.getNgram())) {
        val score = computeFinalScore(entity, mention, messages);
        result.put((mention, entity), score);
      }
    }
	
    return result;
  }
  
  def computeSolution(messages : MessagesMap, max_product : Boolean) : THashMap[Mention, EntityScorePair] = {
    val solution = new THashMap[Mention, EntityScorePair]();

    for (mention <- mentions) {
      var bestScore = 0.0;
      var bestEntity = -1;
      var logSumExpMarginals = 0.0; // for believes/marginals normalization
		
      for (entity <- mentEntsCache.getCandidateEntities(mention.getNgram())) {
        // Score is the log(unnormalized_marginal) for sum-product algorithm and it is 
        // the belief b_i(x_i) for max-product.
        val score = computeFinalScore(entity, mention, messages);
        if (bestEntity == -1 || score > bestScore) {
          bestScore = score;
          bestEntity = entity;
        }

        if (!max_product) {
          if (logSumExpMarginals <= score) {
            val valueToAdd = Math.log(1 + Math.exp(logSumExpMarginals - score));
            logSumExpMarginals = score + valueToAdd;
          } else {
            val valueToAdd = Math.log(1 + Math.exp(score - logSumExpMarginals));
            logSumExpMarginals = logSumExpMarginals + valueToAdd;
          }
        }
      }
		
      if (!max_product) {
        bestScore = bestScore - logSumExpMarginals;
        bestScore = Math.exp(bestScore);
      }
      solution.put(mention, new EntityScorePair(bestEntity, bestScore));
    }
	
    return solution;
  }



  private def computeFinalScore(entity : Int, mention : Mention, messages: MessagesMap) : Double = {
    var rho = 0.0;
    if (rhos.contains(mention.getNgram(), entity)) {
      rho = rhos.getRho(mention.getNgram(), entity);
    } else {
      rho = defaultScorer.rho(mention.getNgram(), entity, false);
    }

    val msgNeighbors = messages.sumNeighborMessages(mention, entity, null, mentions);
    return msgNeighbors + rho;
  }
}
