package md

import index.MentionEntitiesFrequencyIndex
import loopybeliefpropagation.EntityScorePair
import loopybeliefpropagation.ScorerFull
import gnu.trove.map.hash.THashMap
import index.MentEntsFreqIndexWrapper

/**
 * Disambiguates a mention to the most frequent candidate entity.
 */ 
class MostFrequentEntity(mentEntsCache : MentEntsFreqIndexWrapper, scorer : ScorerFull) {

  def solve(mentions : Array[Mention]) : THashMap[Mention, EntityScorePair] = {
    val solution = new THashMap[Mention, EntityScorePair]()
    for (mention <- mentions) {
      solution.put(mention, mentEntsCache.independentDisambiguation(mention.getNgram()))
    }
    return solution
  }
}
