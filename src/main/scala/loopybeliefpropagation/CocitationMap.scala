package loopybeliefpropagation;

import index.AllIndexesBox
import md.Mention
import gnu.trove.map.hash.TLongDoubleHashMap
import utils.Utils
import index.MentEntsFreqIndexWrapper


class CocitationMap {
  
  val map = new TLongDoubleHashMap
  
  ///////////////////////////////////////////////////////////////////
  
  /*
   * Used in piecewise training for finding params of the EL model.
   */
  def this(mentions : Array[String], allIndexesBox : AllIndexesBox, 
      mentEntsCache : MentEntsFreqIndexWrapper, s : Double, cocit_e_e_param : Double) = {
    this()
    Utils.unitTestCompressFull
    
    for (i <- 0 until mentions.size) {
      val mention1 = mentions(i);
      for (j <- i + 1 until mentions.size) {
        val mention2 = mentions(j);
        for (candidate1 <- mentEntsCache.getCandidateEntities(mention1)) {
          for (candidate2 <- mentEntsCache.getCandidateEntities(mention2)) {
            val cocitation = allIndexesBox.wikipEntityCooccurrIndex.getCocitation(candidate1, candidate2, cocit_e_e_param) +
            				s * allIndexesBox.wikilinksEntityCooccurrIndex.getCocitation(candidate1, candidate2, cocit_e_e_param);
            if (cocitation > 0) {
              val pair = Utils.compressTwoInts(Math.min(candidate1, candidate2), Math.max(candidate1, candidate2))
              map.put(pair, cocitation);
            }
          }
 		}
      }
	}
  }


  def this(mentions : Array[Mention], allIndexesBox : AllIndexesBox, 
      mentEntsCache : MentEntsFreqIndexWrapper, s : Double, cocit_e_e_param : Double) = {
    this(mentions.map(m => m.getNgram), allIndexesBox, mentEntsCache, s, cocit_e_e_param)
    Utils.unitTestCompressFull
  }  
  
  /*
   * Used in pseudolikelihood training for finding params of the EL model.
   */
  def this(mentions : Array[String], entities : Array[Int], allIndexesBox : AllIndexesBox, 
      mentEntsCache : MentEntsFreqIndexWrapper, s : Double, cocit_e_e_param : Double) = {
    this()
    Utils.unitTestCompressFull

    assert(mentions.size == entities.size);

    for (i <- 0 until mentions.size) {
      val entity1 = entities(i);
      for (j <- 0 until mentions.size) {
        val mention2 = mentions(j);
        for (candidate2 <- mentEntsCache.getCandidateEntities(mention2)) {
          val cocitation = allIndexesBox.wikipEntityCooccurrIndex.getCocitation(entity1, candidate2, cocit_e_e_param) +
            				s * allIndexesBox.wikilinksEntityCooccurrIndex.getCocitation(entity1, candidate2, cocit_e_e_param);
          if (cocitation > 0) {
            val pair = Utils.compressTwoInts(Math.min(entity1, candidate2), Math.max(entity1, candidate2))
            map.put(pair, cocitation);
          }          
        }
      }
    }
  }

  
  def get(entity1 : Int, entity2 : Int) : Double = {
    val pair = Utils.compressTwoInts(Math.min(entity1, entity2), Math.max(entity1, entity2))
    if (!map.containsKey(pair)) {
      return 0;
    }
    return map.get(pair);
  }

}