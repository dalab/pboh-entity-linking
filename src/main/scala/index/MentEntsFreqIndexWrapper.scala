package index

import gnu.trove.map.hash.THashMap
import loopybeliefpropagation.EntityScorePair
import loopybeliefpropagation.ScorerFull
import loopybeliefpropagation.ScorerWeights
import loopybeliefpropagation.RhoCache
import utils.Normalizer

class MentEntsFreqIndexWrapper(
    allIndexesBox : AllIndexesBox, 
    rhoCache : RhoCache, 
    tkspans : Array[String],  // can be uppercase
    contexts : THashMap[String, Array[Int]], 
    scorerWeights : ScorerWeights) {
  
  val cache = new THashMap[String, CandidatesList] 

  def computeCandidatesForOneName(mm : String) : CandidatesList = {
    val m = Normalizer.normalizeLowercase(mm);
    if (!cache.containsKey(m)) {
      val cands = allIndexesBox.mentionEntsFreqIndex.computeCandidatesForOneName(m)
      if (cands == null) {
        return null
      }
      val expensive = cands._3 // Not used at this moment 
      val correctedMention = cands._1 // Not used at this moment 
      val candidates = cands._2 
      
      if (tkspans != null) {
        for (oo <- tkspans) {
          val other = Normalizer.normalizeLowercase(oo);
          if (other != m && other.contains(m) && containsMention(other)) {
            val otherCands = getCandidateEntities(other)
            for (c <- otherCands) {
              candidates.addEntity(c, 1)
            }
          }
        }
      }

      def rho_funct = (m : String, e : Int) => rhoCache.rho(m, e, candidates.getCandidateProbability(e), false, contexts, scorerWeights, allIndexesBox)

      // Keep only top 10 candidates sorted after their rho(m,e, context) values
      val cand_list = candidates.getCandidateEntities
      
      val sorted_cand_list = cand_list.map(e => (e, -rho_funct(m,e))).sortBy(_._2 ).map(_._1)

      assert(sorted_cand_list(0) == candidates.getBestIndependentEntity(m, rho_funct))
      assert(sorted_cand_list.length == candidates.getCandidateEntitiesCount)


      for (i <- 10 until sorted_cand_list.size) {
        candidates.removeEntity(sorted_cand_list(i))
      }
        
      assert(sorted_cand_list(0) == candidates.getBestIndependentEntity(m, rho_funct))
      
      cache.put(m, candidates)
      candidates
    } else {
      cache.get(m)
    }
  }
    
  def containsMention(name : String) : Boolean = {
    val candidates = computeCandidatesForOneName(name)
    return (candidates != null)
  }
  
  def getCandidateEntities(name : String) : Array[Int] = {
    val candidates = computeCandidatesForOneName(name)
    candidates.getCandidateEntities();
  }  
  
  def getCandidateProbability(name : String, candidate : Int) : Double = {
    val candidates = computeCandidatesForOneName(name) 
    candidates.getCandidateProbability(candidate)
  }  

  def getMostFrequentEntity(name : String) : Int = {
    val candidates = computeCandidatesForOneName(name) 
    candidates.getMostFrequentEntity
  }
  
  // Best entity ordered by rho = log(p(e|m,context))
  def independentDisambiguation(name : String) : EntityScorePair = {
    val candidates = computeCandidatesForOneName(name)
    def rho_funct = (m : String, e : Int) => rhoCache.rho(m, e, candidates.getCandidateProbability(e), false, contexts, scorerWeights, allIndexesBox)
    val entity = candidates.getBestIndependentEntity(name, rho_funct)
    val rho_ent = rho_funct(name, entity)
    return new EntityScorePair(entity, rho_ent);
  }
  
  def getCandidateEntitiesCount(name : String) : Int = {
    val candidates = computeCandidatesForOneName(name)
    candidates.getCandidateEntitiesCount() 
  }
}