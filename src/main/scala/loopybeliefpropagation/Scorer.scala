package loopybeliefpropagation

import md.Mention
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.THashMap

trait Scorer {
  
  def computeMessageScore(from : Mention, to : Mention, entityFrom : Int, entityTo : Int, oldMessages : MessagesMap) : Double
      
  def computeScores(messages: MessagesMap) : TObjectDoubleHashMap[(Mention, Int)]
  
  def computeSolution(messages : MessagesMap, max_product : Boolean) : THashMap[Mention, EntityScorePair]

}