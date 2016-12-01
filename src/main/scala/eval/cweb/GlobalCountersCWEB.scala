package eval.cweb

import index.AllIndexesBox
import index.MentEntsFreqIndexWrapper
import loopybeliefpropagation.ScorerWeights
import loopybeliefpropagation.RhoCache

class GlobalCountersCWEB extends Serializable {
  private var totalNumMentionsScannedSoFar : Int = 0
  private var numNamesNotInCandidatesIndex : Int = 0
  private var numGoogleFreebaseEntitiesNotInWikip : Int = 0
  private var numBothNameNotInIndexAndFreebNotInWikip : Int = 0
  private var numGoogleEntsNotInCandidatesList : Int = 0
  
  private var numGoogleEntsOn1PlaceInCandList : Int = 0
  private var numGoogleEntsOn2PlaceInCandList : Int = 0
  private var numGoogleEntsOn3PlaceInCandList : Int = 0
  private var numGoogleEntsOn4_8PlaceInCandList : Int = 0
  private var numGoogleEntsOn9_16PlaceInCandList : Int = 0
  private var numGoogleEntsOn17_32PlaceInCandList : Int = 0
  private var numGoogleEntsOn33_64PlaceInCandList : Int = 0
  private var numGoogleEntsOnBigger64PlaceInCandList : Int = 0

  def update(mention : String, freebaseId : String, allIndexesBox : AllIndexesBox) = {
    val mentEntsCache = new MentEntsFreqIndexWrapper(allIndexesBox, new RhoCache, null, null, new ScorerWeights)
    
    if (!mentEntsCache.containsMention(mention)) {
      numNamesNotInCandidatesIndex += 1
      println(" Name NOT IN mentionFreqIndex: " + mention)
    } 

    if (!allIndexesBox.freebaseToWikiIDIndex.containsKey(freebaseId)) {
      numGoogleFreebaseEntitiesNotInWikip += 1
    } else {
      val id = allIndexesBox.freebaseToWikiIDIndex.get(freebaseId).intValue()
      if (mentEntsCache.containsMention(mention)) {
        val candList = mentEntsCache.getCandidateEntities(mention)
        val candMap = new collection.mutable.HashMap[Int,Double] 
        for (c <- candList) {
          candMap += ((c, mentEntsCache.getCandidateProbability(mention, c)))
        }
        val candidates = candMap.toList.sortBy(_._2).reverse.map(_._1)
            
        if (!candidates.contains(id)) {
          println(" REAL ENT Not in mentionFreqIndex : " + mention + " --- " +
              id + " : " + allIndexesBox.entIDToNameIndex.get(id))
          numGoogleEntsNotInCandidatesList += 1
        } else {
          val index : Int = candidates.indexOf(id)
          if (index == 0) numGoogleEntsOn1PlaceInCandList += 1
          if (index == 1) numGoogleEntsOn2PlaceInCandList += 1
          if (index == 2) numGoogleEntsOn3PlaceInCandList += 1
          if (index >= 3 && index < 8) numGoogleEntsOn4_8PlaceInCandList += 1
          if (index >= 8 && index < 16) numGoogleEntsOn9_16PlaceInCandList += 1
          if (index >= 16 && index < 32) numGoogleEntsOn17_32PlaceInCandList += 1
          if (index >= 32 && index < 64) numGoogleEntsOn33_64PlaceInCandList += 1
          if (index >= 64) numGoogleEntsOnBigger64PlaceInCandList += 1
        }
      }
    }
    if (!mentEntsCache.containsMention(mention) &&
        !allIndexesBox.freebaseToWikiIDIndex.containsKey(freebaseId)) {
      numBothNameNotInIndexAndFreebNotInWikip  += 1
    } 
    totalNumMentionsScannedSoFar += 1
  }
  
  def printAll = {
    var x : Double = 0.0 
    println("************** Global counters *******************")
    println("total Num Mentions Scanned So Far = " + totalNumMentionsScannedSoFar)
    
    x = 100.0 * numNamesNotInCandidatesIndex / totalNumMentionsScannedSoFar
    println("perc Mentions Not In Our Indexes = " + "%4.2f".format(x) +
        "% ; num =" + numNamesNotInCandidatesIndex)
        
    x = 100.0 * numGoogleFreebaseEntitiesNotInWikip / totalNumMentionsScannedSoFar    
    println("perc Freebase Ents Not In Wikip = " + "%4.2f".format(x) +
        "% ; num =" + numGoogleFreebaseEntitiesNotInWikip)
    
    x = 100.0 * numBothNameNotInIndexAndFreebNotInWikip  / totalNumMentionsScannedSoFar    
    println("perc Both Mention Not In Index And Freeb Not In Wikip = "  +
        "%4.2f".format(x) + "% ; num =" + numBothNameNotInIndexAndFreebNotInWikip )

    x = 100.0 * ( numGoogleFreebaseEntitiesNotInWikip  - numBothNameNotInIndexAndFreebNotInWikip ) / totalNumMentionsScannedSoFar 
    println("perc Mention In Index, But Freeb Not In Wikip = " +
        "%4.2f".format(x) + "% ; num =" + 
        ( numGoogleFreebaseEntitiesNotInWikip  - numBothNameNotInIndexAndFreebNotInWikip ))
    

    ////////////////////////////////////////////////////////////////
    val numitor = totalNumMentionsScannedSoFar  - 
      numNamesNotInCandidatesIndex  - 
      numGoogleFreebaseEntitiesNotInWikip  +
      numBothNameNotInIndexAndFreebNotInWikip 
      
    x = 100.0 * numGoogleEntsNotInCandidatesList  / numitor
    println("Showing percentages from mentions that are in indexes and freebase ents that are Wikipedia")
    println("perc Google Ents Not In Candidates List (max is 100%) = "  + "%4.2f".format(x) +
        "% ; num =" + numGoogleEntsNotInCandidatesList )
    
    println("num Google Ents On this Place in Cand List (max is 100%):")
    
    x = 100.0 * numGoogleEntsOn1PlaceInCandList  / numitor
    println("  1 --> " + "%4.2f".format(x) + "% ; num =" +
        numGoogleEntsOn1PlaceInCandList )
    
    x = 100.0 * numGoogleEntsOn2PlaceInCandList  / numitor
    println("  2 --> " + "%4.2f".format(x) + "% ; num =" +
        numGoogleEntsOn2PlaceInCandList )
    
    x = 100.0 * numGoogleEntsOn3PlaceInCandList  / numitor
    println("  3 --> " + "%4.3f".format(x) + "% ; num =" +
        numGoogleEntsOn3PlaceInCandList )
    
    x = 100.0 * numGoogleEntsOn4_8PlaceInCandList  / numitor
    println("  4 : 8 --> " + "%4.3f".format(x) + "% ; num =" +
        numGoogleEntsOn4_8PlaceInCandList )
    
    x = 100.0 *numGoogleEntsOn9_16PlaceInCandList  / numitor
    println("  9:16 --> " + "%4.4f".format(x) + "% ; num =" +
        numGoogleEntsOn9_16PlaceInCandList )
    
    x = 100.0 * numGoogleEntsOn17_32PlaceInCandList  / numitor
    println("  17:32 --> " + "%4.4f".format(x) + "% ; num =" +
        numGoogleEntsOn17_32PlaceInCandList )
    
    x = 100.0 * numGoogleEntsOn33_64PlaceInCandList  / numitor
    println("  33:64 --> " + "%4.4f".format(x) + "% ; num =" +
        numGoogleEntsOn33_64PlaceInCandList )
    
    x = 100.0 * numGoogleEntsOnBigger64PlaceInCandList  / numitor
    println("  more than 64 --> " + "%4.4f".format(x) + "% ; num =" +
        numGoogleEntsOnBigger64PlaceInCandList )
    println("***************************************************")
  }
  
  def +=(gc : GlobalCountersCWEB) = {
    totalNumMentionsScannedSoFar += gc.totalNumMentionsScannedSoFar 
    numNamesNotInCandidatesIndex += gc.numNamesNotInCandidatesIndex 
    numGoogleFreebaseEntitiesNotInWikip += gc.numGoogleFreebaseEntitiesNotInWikip 
    numBothNameNotInIndexAndFreebNotInWikip += gc.numBothNameNotInIndexAndFreebNotInWikip 
    numGoogleEntsNotInCandidatesList += gc.numGoogleEntsNotInCandidatesList 

    numGoogleEntsOn1PlaceInCandList += gc.numGoogleEntsOn1PlaceInCandList 
    numGoogleEntsOn2PlaceInCandList += gc.numGoogleEntsOn2PlaceInCandList 
    numGoogleEntsOn3PlaceInCandList += gc.numGoogleEntsOn3PlaceInCandList 
    numGoogleEntsOn4_8PlaceInCandList += gc.numGoogleEntsOn4_8PlaceInCandList 
    numGoogleEntsOn9_16PlaceInCandList += gc.numGoogleEntsOn9_16PlaceInCandList 
    numGoogleEntsOn17_32PlaceInCandList += gc.numGoogleEntsOn17_32PlaceInCandList 
    numGoogleEntsOn33_64PlaceInCandList += gc.numGoogleEntsOn33_64PlaceInCandList 
    numGoogleEntsOnBigger64PlaceInCandList += gc.numGoogleEntsOnBigger64PlaceInCandList 
  }
}
