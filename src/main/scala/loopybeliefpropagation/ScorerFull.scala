package loopybeliefpropagation;

import index.AllIndexesBox
import md.Mention
import gnu.trove.map.hash.THashMap
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import index.MentEntsFreqIndexWrapper

class RhoCache {
 
  // Keep the rho values in cache since they are expensive to compute.
  private val rho_cache = new TObjectDoubleHashMap[(String,Int)] 
  
  def rho(
      m : String, 
      e : Int,
      p_hat_e_cond_m : Double,
      printDebug : Boolean, 
      contexts : THashMap[String, Array[Int]], 
      scorerWeights : ScorerWeights,
      allIndexesBox : AllIndexesBox) : Double = {

    if (p_hat_e_cond_m == 0) {
      return 0.0; 
      // This shouldn't happen anywhere except ConsoleEL, 
      // since the candidates for a mention are selected to have p(e|m) > 0
    }
    
    if (rho_cache.containsKey((m,e))) {
      return rho_cache.get((m,e))
    }
    
    var result = Math.log(p_hat_e_cond_m);

    assert (scorerWeights.b >= 0);
    assert(scorerWeights.xi > 0); // Otherwise, we get log(0).

    // Add to result, for each context word, the value log(p(w|e)/p(w)) if this ratio is >= 1.
    if (contexts != null && contexts.containsKey(m)) {
      val context = contexts.get(m);

      // We add b * log(p~(w|e) / p^(w)) to the final result, for each context word w.
      for (word <- context) {
        assert(allIndexesBox.wordFreqDict.wordToFreq.containsKey(word))
        
        val uprob_w = allIndexesBox.wordFreqDict.wordToFreq.get(word);
        val Z_w = allIndexesBox.wordFreqDict.totalSumFreqs;
        val p_hat_w = (0.0 + uprob_w) / Z_w;

        var sum_p_tilda_w_e = 0.0;
        if (allIndexesBox.wordEntityProbsIndex.entToTotal.containsKey(e)) {
          sum_p_tilda_w_e = allIndexesBox.wordEntityProbsIndex.sum_unnorm_p_tilda_w_e.get(e);
          // Absolute discounting:
          sum_p_tilda_w_e -= scorerWeights.xi * allIndexesBox.wordEntityProbsIndex.entToNumNonzeroWordProbs.get(e)

          sum_p_tilda_w_e /= allIndexesBox.wordEntityProbsIndex.entToTotal.get(e)
          
          assert(sum_p_tilda_w_e < 1.0 && sum_p_tilda_w_e >= 0.0, " Entity e = " + e + " sum_p_tilda_w_e = " + sum_p_tilda_w_e +
            " sum tilda before substract = " + allIndexesBox.wordEntityProbsIndex.sum_unnorm_p_tilda_w_e.get(e) + 
            " num nonzero word probs = " + allIndexesBox.wordEntityProbsIndex.entToNumNonzeroWordProbs.get(e))
        }
        
        assert(scorerWeights.xi < 1.0)
        assert(scorerWeights.delta_w_e <= 1.0)
        
        val miu_e = 1.0 - scorerWeights.delta_w_e * sum_p_tilda_w_e;

        assert(miu_e > 0.0 && miu_e <= 1.0, 
            "Delta = " + scorerWeights.delta_w_e + "  e = " + e + " sum_p_tilda_w_e = " + sum_p_tilda_w_e);
        
        var p_w_e_over_p_hat_w = miu_e;
        var p_hat_w_e = 0.0
        if (allIndexesBox.wordEntityProbsIndex.containsWordEntityFreq(word, e) &&
              allIndexesBox.wordEntityProbsIndex.entToTotal.containsKey(e)) {
          val uprob_w_e = allIndexesBox.wordEntityProbsIndex.getWordEntityFreq(word, e);
          assert(uprob_w_e > 0)
          val Z_w_e = allIndexesBox.wordEntityProbsIndex.entToTotal.get(e);
          assert(Z_w_e > 0)
          p_hat_w_e = (uprob_w_e.toDouble - scorerWeights.xi) / Z_w_e; // Absolute discounting
          p_w_e_over_p_hat_w = scorerWeights.delta_w_e * p_hat_w_e / p_hat_w + miu_e;
        }

        assert(p_w_e_over_p_hat_w > 0.0)
        result += scorerWeights.b * Math.log(p_w_e_over_p_hat_w);
        
        if (printDebug) {
          val w = allIndexesBox.wordFreqDict.wordIDtoString.get(word)
          println("p(w = " + w + " | e = " + allIndexesBox.entIDToNameIndex.get(e) + ") / p(w)= " + p_w_e_over_p_hat_w)
          println("p_hat(w = " + w + ") = " + p_hat_w)
          println("p(w = " + w + " | e = " + allIndexesBox.entIDToNameIndex.get(e) + ")= " + p_hat_w_e)
          println
        }
      }
    }
  
    if (printDebug) {
      println(" ****** Sum context words for e = " + allIndexesBox.entIDToNameIndex.get(e) + " is " + result)
      println
    }

    rho_cache.put((m,e), result)
    return result;
  }  
}



/*
 * contexts: map m -> contexts
 * scorerWeights : Parameters learned with SGD Pseudolikelihood
 */
class ScorerFull(
    contexts : THashMap[String, Array[Int]], 
    allIndexesBox : AllIndexesBox,
    val rhoCache : RhoCache,
    val mentEntsCache : MentEntsFreqIndexWrapper,
    val scorerWeights : ScorerWeights) extends Scorer {

  private var mentions : Array[Mention] = null
  
  private var cocitationMap : CocitationMap = null;

  
  /*
   * Used in VerifyEDLBPForListOfWords.java and in SGD
   */
  def this(mentions : Array[Mention],
      contexts : THashMap[String, Array[Int]],
      allIndexesBox : AllIndexesBox,
      rhoCache : RhoCache,
      mentEntsCache : MentEntsFreqIndexWrapper,
      scorerWeights : ScorerWeights) {
    
    this(contexts, allIndexesBox, rhoCache, mentEntsCache, scorerWeights)
    this.mentions = mentions
	this.cocitationMap = new CocitationMap(mentions, this.allIndexesBox, mentEntsCache, scorerWeights.s, scorerWeights.cocit_e_e_param);
  }

  /*
   * Used in learning SGD params with Piecewise training.
   */
  def this(mentions : Array[String], 
      contexts : THashMap[String, Array[Int]],
      allIndexesBox : AllIndexesBox,
      rhoCache : RhoCache,
      mentEntsCache : MentEntsFreqIndexWrapper,
      scorerWeights : ScorerWeights) {

    this(contexts, allIndexesBox, rhoCache, mentEntsCache, scorerWeights)
    this.cocitationMap = new CocitationMap(mentions, this.allIndexesBox, mentEntsCache, scorerWeights.s, scorerWeights.cocit_e_e_param);
  }
  
  
  /*
   * Used in learning SGD params with Pseudolikelihood training.
   */
  def this(mentions : Array[String],
      entities : Array[Int],
      contexts : THashMap[String, Array[Int]],
      allIndexesBox : AllIndexesBox,
      rhoCache : RhoCache,
      mentEntsCache : MentEntsFreqIndexWrapper,      
      scorerWeights : ScorerWeights) {
    
    this(contexts, allIndexesBox, rhoCache, mentEntsCache, scorerWeights)
    this.cocitationMap = new CocitationMap(mentions, entities, this.allIndexesBox, mentEntsCache, scorerWeights.s, scorerWeights.cocit_e_e_param);
  }

  
  /*
   * Cocitation #(e1,e2) - number of co-occurrences of e1 and e2 in the corpus.
   * Used here and in the scala learning project.
   */
  def cocitation(e1 : Int, e2 : Int) : Double = {
	  return cocitationMap.get(e1, e2);
  }

  /*
   * Popularity: p^(e) unnormalized - measured empirically by counting.
   * Used here and in the scala learning project.
   */
  def p_hat(e : Int) : Double = {
    return allIndexesBox.wikipEntityCooccurrIndex.getPopularity(e) +
    		scorerWeights.s * allIndexesBox.wikilinksEntityCooccurrIndex.getPopularity(e);
  }

  
  def rho(m : String, e : Int, printDebug : Boolean) : Double = {
    // p^(e|m) measured empirically by counting.
    val p_hat_e_cond_m = mentEntsCache.getCandidateProbability(m, e);
    return rhoCache.rho(m, e, p_hat_e_cond_m, printDebug, contexts, scorerWeights, allIndexesBox)  
  }   
  
  
  /*
   * Independence prior : NUM_PAIRS * p^(e1) * p^(e2)
   * Used here and in the scala learning project.
   */
  def independence_prior(e1 : Int, e2 : Int) : Double = {
    val numPairs = 0.0 + allIndexesBox.wikipEntityCooccurrIndex.getTotalNumPairs() +
    		scorerWeights.s * allIndexesBox.wikilinksEntityCooccurrIndex.getTotalNumPairs();
    return p_hat(e1) * p_hat(e2) / numPairs;
  }

  
  def lambda(e1 : Int, e2 : Int, printDebug : Boolean) : Double = {
    val numPairs = 0.0 + allIndexesBox.wikipEntityCooccurrIndex.getTotalNumPairs() +
    		scorerWeights.s * allIndexesBox.wikilinksEntityCooccurrIndex.getTotalNumPairs();

    val numDistPairs = allIndexesBox.wikilinksEntityCooccurrIndex.numDistinctPairs
    val cocit = Math.max(0.0, cocitation(e1, e2) - scorerWeights.delta_cocit)  +
    		(numPairs - scorerWeights.delta_cocit * numDistPairs) / numPairs * p_hat(e1) / numPairs * p_hat(e2);
    
    if (printDebug) {
      val p_hat_e1 = p_hat(e1).toDouble / numPairs
      val lamb = Math.log(cocit+0.00001) - scorerWeights.h * Math.log(p_hat(e1)) - scorerWeights.h * Math.log(p_hat(e2));
      println("      raw cocit = " + cocitation(e1, e2) + " smoothed cocit = " + cocit + " pop(" + allIndexesBox.entIDToNameIndex.get(e1) + ") = " +  p_hat(e1) + 
          " pop(" + allIndexesBox.entIDToNameIndex.get(e2) + ") = " +  p_hat(e2) + " numPairs = " + numPairs + " final lambda = " + lamb)
    }

    if (cocit > 0) {
      return Math.log(cocit) - scorerWeights.h * Math.log(p_hat(e1)) - scorerWeights.h * Math.log(p_hat(e2));
    } else {
      return -1000 - scorerWeights.h * Math.log(p_hat(e1)) - scorerWeights.h * Math.log(p_hat(e2));
    }
  }

  
  /////////////////////// LBP functions ///////////////////////////////////////
  def computeMessageScore(
      from : Mention, 
      to : Mention, 
      entityFrom : Int, 
      entityTo : Int, 
      oldMessages : MessagesMap) : Double = {
    
    assert(mentions != null)
    
    val l = mentions.size;
    val rho_score = scorerWeights.f * rho(from.getNgram(), entityFrom, false);
    
    assert (!rho_score.isInfinite() && !rho_score.isNaN, 
		  "rho = " + rho(from.getNgram(), entityFrom, false) + " ; m = " + from.getNgram() + " e = " + entityFrom);		

    val lambda_val = lambda(entityFrom, entityTo, false) * scorerWeights.g * 2.0  / (l-1)
    
    assert (!lambda_val.isInfinite() && !lambda_val.isNaN, 
        "lambda = " + lambda(entityFrom, entityTo, false) + " ; e1 = " + entityFrom + " e2 = " + entityTo +
        " p_hat(e1) = " + p_hat(entityFrom) + " p_hat(e2) = " + p_hat(entityTo) + " l = " + l);		

    val messagesNeighbors = oldMessages.sumNeighborMessages(from, entityFrom, to, mentions);
    val result = messagesNeighbors + rho_score + lambda_val;

    /*
    println("MESSAGE mention from = " + from.getNgram + "; entityFrom = " + entityFrom + " :::: mention to = " + to.getNgram + "; entityTo = " + entityTo + 
        "; rho(from,e_from) = " + rho_score + "; lambda(e_from,e_to) = " + lambda_val + " sum_msg_neigh = " + messagesNeighbors + " result = " + result)
    * 
    */
    return result;
  }

  private def computeFinalScore(entity : Int, mention : Mention, messages : MessagesMap) : Double = {
    val rho_contrib = scorerWeights.f * rho(mention.getNgram(), entity, false);
    val msgNeighbors = messages.sumNeighborMessages(mention, entity, null, mentions);
    return msgNeighbors + rho_contrib;
  }	

  def computeScores(messages: MessagesMap) : TObjectDoubleHashMap[(Mention, Int)] = {
    val result = new TObjectDoubleHashMap[(Mention, Int)]();
    for (mention <- mentions) {
      for (entity <- mentEntsCache.getCandidateEntities(mention.getNgram)) {
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
      var logSumExpMarginals = -10000.0; // for believes/marginals normalization

      for (entity <- mentEntsCache.getCandidateEntities(mention.getNgram)) {
        // Score is the log(unnormalized_marginal) for sum-product algorithm and it is
        // the belief b_i(x_i) for max-product.
        val score = computeFinalScore(entity, mention, messages);

        if (bestEntity == -1 || score > bestScore) {
          bestScore = score;
          bestEntity = entity;
        }
        
        if (!max_product) {
          if (logSumExpMarginals == -10000.0) {
            logSumExpMarginals = score;
          } else if (logSumExpMarginals <= score) {
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

  // For debug only.
  private def logPosterior(input : TObjectIntHashMap[Mention]) : Double = {
    val l = input.size;
    
    var score = 0.0;
    for (mention <- input.keys(Array.empty[Mention])) {
      val entity = input.get(mention);
      score += scorerWeights.f * rho(mention.getNgram(), entity, false);
    }
    
    val mentionKeys = input.keys;
    for (i <- 0 until mentionKeys.length) {
      for (j <- i + 1 until mentionKeys.length) {
        score += lambda(input.get(mentionKeys(i)), input.get(mentionKeys(j)), false) * scorerWeights.g * 2.0  / (l-1);
      }
    }
    return score;
  }	

	
  // Just for DEBUG:
  def printScorerParams(source : String, entity1 : Int, mention1 : String, entity2 : Int) = {
    val e1 = allIndexesBox.entIDToNameIndex.get(entity1)
    val e2 = allIndexesBox.entIDToNameIndex.get(entity2)
    
    val lambda_val = lambda(entity1, entity2, false)
    val beta1 = rho(mention1, entity1, false)
    val x = beta1 + lambda_val
    
    val numPairs = 0.0 + allIndexesBox.wikipEntityCooccurrIndex.getTotalNumPairs() +
    	scorerWeights.s * allIndexesBox.wikilinksEntityCooccurrIndex.getTotalNumPairs()
    val numLinks = 0.0 + allIndexesBox.wikipEntityCooccurrIndex.getTotalNumLinks() +
    	scorerWeights.s * allIndexesBox.wikilinksEntityCooccurrIndex.getTotalNumLinks();
		
    println("   " + source + ": COCITATION: e1= " + e1 + " ;pop1=" + p_hat(entity1) +
        " ;e2= " + e2 + " ;pop2=" + p_hat(entity2) +
        " ;cocitation=" + cocitation(entity1, entity2))
        
    println("   " + source + ": lambda(e1,e2)=" + lambda_val +
        " ;rho(m1,e1) = " + beta1 +
        " ;log p(e1|m1) + (lambda(e1,e2) or const) = " + x);
	}  
}
