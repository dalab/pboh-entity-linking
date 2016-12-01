package index;

import gnu.trove.map.hash.THashMap
import gnu.trove.set.hash.THashSet
import scala.collection.mutable.ArrayBuffer
import context.PorterStemmer


/**
 * Maps mentions to keyphraseness and candidate entities list.
 * The file loaded should be produced by @see knowledgebase.MentionEntitiesKeyphrasenessIndexBuilder
 * 
 * File lines format: name A  B  C  D1,E1  D2,E2 ... DN,EN
 * A - num docs that contain a link with text name and any entity (possibly disambiguation, list 
 *     pages  or inexistent - red links)
 * B - num docs that contain name with or without link
 * C - Total number of times the corresponding name is linked (may be bigger than the number of
 *     documents in which it is linked).
 * Di, Ei - pairs ent,freq - how many times (not docs) ent Di appeared linked to name
 * This list is prunned to contain just entities appearing at least 2% * C
 */
class MentionEntitiesFrequencyIndex extends java.io.Serializable {
  
  private val INITIAL_SIZE = 21000000;

  private var map : THashMap[String, Int] = null
  private var listOfListsOfCand : Array[CandidatesList] = null
  private var sortedMentions : Array[String] = null
  
  
///////////////////////////////////////////////////////////////////////////////
  
  
  def load(path : String, entIDToNameIndex : EntIDToNameIndex) = {
	  System.err.println("Loading mention index:" + path)
	  map = new THashMap[String, Int](INITIAL_SIZE * 4 / 3 + 1)
	  var candsListofList = new ArrayBuffer[CandidatesList](INITIAL_SIZE * 4 / 3 + 1)
	  
	  val lines = scala.io.Source.fromFile(path).getLines
	  val mentions = new ArrayBuffer[String]
			  
	  var cnt = 0
	  var idx = 0
	  lines.foreach(line => {
	    cnt = cnt + 1
	    if (cnt % 300000 == 0) {
	      System.err.println("Processed " + cnt + " lines.")
	    }
	    val elements = line.split("\t", 4)
	    val name = elements(0)
	    val candidates = new CandidatesList(elements(3), name, entIDToNameIndex)

	    // Skip mentions contained in less than MINIMUM_COUNT docs.
	    if (candidates.getTotalFrequency > 0) {
	      mentions += name
        candsListofList += candidates
	      map.put(name, idx)
	      idx = idx + 1
	    }
	  })
	  
	  
	  listOfListsOfCand = candsListofList.toArray

	  System.err.println("Now sorting in memory all strings")
	  val t1 = System.currentTimeMillis
	  sortedMentions = mentions.sortWith((e1, e2) => (e1 compareTo e2) < 0).toArray
	  for (i <- 0 until sortedMentions.size) {
	    listOfListsOfCand(map.get(sortedMentions(i))).setPosInSortedArray(i)
	  }

	  val t2 = System.currentTimeMillis
	  System.err.println("Done sorting. time = " + (t2 - t1) / 1000.0)

	  assert(sortedMentions.size == map.size())
	  System.err.println("Loaded mention index. Size = " + map.size())
  }

  
  
  // Hack to decide if two strings are similar or not.
  private def stringSimilarity(aa : String, bb : String) : Double = {
    var a = PorterStemmer.stem(aa)
    var b = PorterStemmer.stem(bb)
    
    val trigramsInA = new THashSet[String]
    for (i <- 2 until a.length()) {
      trigramsInA.add("" + a(i-2) + a(i-1) + a(i))
    }
    if (a.length() >= 2) {
      trigramsInA.add("$$$" + a(0) + a(1))
      trigramsInA.add(a(a.length() - 2) + a(a.length() - 1) + "###")
    }

    var nrIntersect = 0.0
    var nrReunion = trigramsInA.size()
    
    for (i <- 2 until b.length()) {
      if (trigramsInA.contains("" + b(i-2) + b(i-1) + b(i))) {
        nrIntersect += 1
      } else {
        nrReunion += 1
      }
    }
    if (b.length() >= 2) {
      if (trigramsInA.contains("$$$" + b(0) + b(1))) {
        nrIntersect += 1
      } else {
        nrReunion += 1
      }      
      
      if (trigramsInA.contains(b(b.length() - 2) + b(b.length() - 1) + "###")) {
        nrIntersect += 1
      } else {
        nrReunion += 1
      }      
    }
    return nrIntersect / nrReunion
  }
  
  
  // Finds the index of the closest string to name in the dictionary.
  private def binarySearch(name : String) : String = {
    // Binary Search to find the position where the mention should be
    var start = 0
    var end = sortedMentions.size - 1
    while (start < end) {
      val mid = start + (end - start) / 2
      if (sortedMentions(mid) < name) {
        start = mid + 1
      } else {
        assert(sortedMentions(mid) > name)
        end = mid - 1
      }
    }
    sortedMentions(start)    
  }
    

  // Returns the new name , its candidate lists and a boolean being true if 
  // the operation is expensive (and should be made rarely) and false if not
  def computeCandidatesForOneName(m : String) : (String, CandidatesList, Boolean) = {
    /*
     *  If this name is not very popular, we find the neighboring mention in the
     *  index that is the closest to it.
     */
    var name = m
    var freq = 0
    var expensive = false
    if (!map.containsKey(m)) {
      name = binarySearch(m)
      expensive = true
    } else {
      freq = listOfListsOfCand(map.get(name)).getTotalFrequency
    }
    
    assert(map.containsKey(name))
    
    if (freq == 0) {
      val i = listOfListsOfCand(map.get(name)).getPosInSortedArray
      var nameScore = 0.0
      
      for (j <- Math.max(0, i - 20) to Math.min(sortedMentions.size - 1, i + 20)) {
        val stringSim = stringSimilarity(sortedMentions(j), m)
        if (stringSim > nameScore) {
          nameScore = stringSim
          name = sortedMentions(j)
        }
      }
   
      if (nameScore >= 0.5) {
        expensive = true
        println("Correcting name = " + m + " -- with new name = " + name + " cands = " + map.get(name).toString)
        (name, listOfListsOfCand(map.get(name)).clone, expensive)
      } else {
        null
      }
    } else {
      assert(name == m)
      (name, listOfListsOfCand(map.get(name)).clone, expensive)
    }
  }

  
  /* OOOOOOOOOOOOLD CODE THAT ONLY LOOKS AT THE ENTRIES IN DICTIONARY AND DOES NOT DO ANY MERGING
  def containsMention(name : String) : Boolean = {
    map.containsKey(name)
  }
  
  def getCandidateEntities(name : String) : Array[Int] = {
    if (map.containsKey(name)) {
      return map.get(name).getCandidateEntities();
    } else {
      return Array.empty[Int]
    }
  }
  

  def getCandidateProbability(name : String, candidate : Int) : Double = {
    if (map.containsKey(name)) {
      return map.get(name).getCandidateProbability(candidate);
    } else {
      return 0;
    }
  }

  def getMostFrequentEntity(name : String) : Int = {
    if (map.containsKey(name)) {
      return map.get(name).getMostFrequentEntity();
    } else {
      return -1
    }
  }

  def getMostFrequentEntityWithScore(name : String) : EntityScorePair = {
    if (map.containsKey(name)) {
      val entity = map.get(name).getMostFrequentEntity();
      return new EntityScorePair(entity, getCandidateProbability(name, entity));
    } else {
      return new EntityScorePair(-1,0)
    }
  }
  
  def independentDisambiguation(mention : String, scorer : ScorerFull) : EntityScorePair = {
    if (map.containsKey(mention)) {
      val entity = map.get(mention).getBestIndependentEntity(mention, scorer);
      return new EntityScorePair(entity, scorer.rho(mention, entity, false));
    } else {
      return new EntityScorePair(-1,0)
    }
  }
  
  def getCandidateEntitiesCount(name : String) : Int = {
    if (map.containsKey(name)) {
      return map.get(name).getCandidateEntitiesCount();
    } else {
      return 0
    }
  }
*/
}
