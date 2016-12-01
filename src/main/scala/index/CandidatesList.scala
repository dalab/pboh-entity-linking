package index

import gnu.trove.map.hash.TIntIntHashMap
import java.io.Serializable
import org.apache.commons.lang3.StringUtils
import com.google.common.base.Joiner

/*
 * @param: nameFrequency = Total number of times the corresponding name is 
 * linked (may be bigger than the number of documents in which it is linked).
 * 
 */
class CandidatesList extends Serializable {
  private val CANDIDATE_SEPARATOR = "\t";
  private val ENTRY_SEPARATOR = ",";
  
  private var totalFrequency : Int = 0
  private val topCandidates = new TIntIntHashMap(64);

  private var posInSortedArray = -1 // Position of mention (the key) in the sorted array of mentions
  
///////////////////////////////////////////////////////////////////////////////
	
  /**
   * Recreates an object from the textual representation obtained by using toString() method.
   */
  def this(text : String, name : String, entIDToNameIndex : EntIDToNameIndex) {
    this()
    totalFrequency = 0
    val parts = StringUtils.split(text, CANDIDATE_SEPARATOR);
    //val totalFreq = Integer.parseInt(parts(0))
    assert(!parts(0).contains(ENTRY_SEPARATOR))
    
    for (i <- 1 until parts.length) {
      val part = parts(i)
      val subparts = StringUtils.split(part, ENTRY_SEPARATOR);
      val entity = Integer.parseInt(subparts(0));
      val frequency = Integer.parseInt(subparts(1));
      if (entIDToNameIndex.containsKey(entity)) {
        totalFrequency += frequency;
        topCandidates.put(entity, frequency);
      }
    }
  }
  
  def this(other : CandidatesList) {
    this()
    totalFrequency = other.totalFrequency
    for (entity <- other.topCandidates.keys()) {
      topCandidates.put(entity, other.topCandidates.get(entity))
    }
  }

  def addEntity(ent : Int, freq : Int) = {
    if (!topCandidates.containsKey(ent)) {
      totalFrequency += freq
      topCandidates.put(ent, freq)      
    }
  }
  
  def removeEntity(ent : Int) {
    topCandidates.remove(ent)
  }
  
  def setPosInSortedArray(x : Int) = {
    posInSortedArray = x
  }
  
  def getPosInSortedArray() = {
    posInSortedArray
  }  
  
  
  def getTotalFrequency() : Int = {
    return totalFrequency;
  }
	
  def getCandidateEntities() : Array[Int] = {
    return topCandidates.keys
  }
  
  def getCandidateEntitiesCount() : Int = {
    return topCandidates.size();
  }
	
  
  def getCandidateFrequency(entity : Int) : Int = {
    if (containsCandidate(entity)) {
      return topCandidates.get(entity);
    } else {
      return 0;
    }
  }  

  def getCandidateProbability(candidate : Int) : Double = {
    val candidateFrequency = getCandidateFrequency(candidate);
    return candidateFrequency.toDouble / totalFrequency;
  }
 
  
  def getMostFrequentEntity() : Int = {
    var bestEntity = -1;
    var bestFrequency = -1;
    for (entity <- topCandidates.keys()) {
      val frequency = topCandidates.get(entity);
      if (frequency > bestFrequency) {
        bestFrequency = frequency;
        bestEntity = entity;
      }
    }
    return bestEntity;
  }
  
  def getBestIndependentEntity(mention : String, rho_funct : (String,Int) => Double) : Int = {
    var bestEntity = -1;
    var bestScore = -10000.0;
    for (entity <- topCandidates.keys()) {
      val score = rho_funct(mention, entity);
      if (score > bestScore) {
        bestScore = score;
        bestEntity = entity;
      }
    }
    return bestEntity;
  }


  def containsCandidate(entity : Int) : Boolean = {
    return topCandidates.containsKey(entity);
  }	
 
  override def toString() : String = {
    val candidates = new StringBuilder("");
    for (entity <- topCandidates.keys()) {
      candidates.append(entity + ENTRY_SEPARATOR + topCandidates.get(entity) + CANDIDATE_SEPARATOR);
    }
    return Joiner.on(CANDIDATE_SEPARATOR).join(totalFrequency, candidates);
  }
  
  override def clone() : CandidatesList = {
    new CandidatesList(this)
  }
}
