package md

import gnu.trove.map.hash.TIntDoubleHashMap
import utils.Normalizer


/*
 * Encapsulates an ngram mention of an entity. 
 * The original text is a substring in the input of the given offset and length.
 * Computes keyphraseness, document frequency, candidate entities with compatibility scores and a
 * unique negative id.
 */
class Mention(originalNgram : String, offset : Int, length : Int) {
  val WINDOW_SIZE = 50;

  private val ngram = Normalizer.normalizeLowercase(originalNgram);
  
  private var groundTruthEntity = -1;

  //////////////////////////////////////////////////////////////////////
  
  def this(originalNgram : String, offset : Int) = {
    this(originalNgram, offset, originalNgram.length)
  }	

  //////////////////////////////////////////////////////////////////////
  
  def getGroundTruthEntity() : Int = {
    return groundTruthEntity;
  }

  def setGroundTruthEntity(groundTruthEntity : Int) = {
    this.groundTruthEntity = groundTruthEntity;
  }
  
  def getNgram() : String = {
    return ngram;
  }

  def getOffset() : Int = {
    return offset;
  }
	
  def getLength() : Int = {
    return length;
  }
  
  def getOriginalNgram() : String = {
    return originalNgram;
  }

  
  override def equals(obj : Any) : Boolean = {
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    
    val other = obj.asInstanceOf[Mention];
    return ngram.equals(other.getNgram()) &&
    		offset == other.getOffset() &&
    		length == other.getLength();
  }

  override def hashCode() : Int = {
    return ngram.hashCode() + offset + length;
  }

  override def toString() : String = {
    return "ngram:" + ngram + " offset: " + offset + " length:" + length +
    		" original ngram:" + originalNgram; 
	}
}
