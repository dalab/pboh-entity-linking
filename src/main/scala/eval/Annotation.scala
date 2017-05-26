package eval

import index.EntIDToNameIndex
import md.Mention

/**
 *  Wikipedia entity annotation for a token span.
 *  The token span can be found in the specified file by using the given offset and length.
 *  
 *  @param: score = confidence score for this entity
 */
class Annotation(
    val entity : Int, 
    val score : Double, 
    val mention : Mention,
    val groundTruthEntity : Int) extends java.io.Serializable {

  private var filePath = "";

  // For IITB, tokenSpan should be null. For others, should be filled in.
  def this(entity : Int,
      score : Double,
      mention : Mention, 
      filePath : String,
      groundTruthEntity : Int) = {
    this(entity, score, mention, groundTruthEntity)
    this.filePath = filePath;
  }	

  def getEntity() : Int = entity

  def getGroundTruthEntity() : Int = groundTruthEntity
  
  def getScore() : Double = score

  def getMention() = mention
  
  def getFilePath() = filePath

  override def toString() : String = {
    return "entity:" + entity + " score: " + score +
    		" mention:" + mention + " filepath:" + filePath;
  }

  override def equals(obj : Any) : Boolean = {
    if (obj == null || !obj.isInstanceOf[Annotation]) {
      return false;
    }
    val other = obj.asInstanceOf[Annotation]
    return mention.equals(other.mention) &&
    	filePath.equals(other.filePath) &&
    	entity == other.entity &&
    	groundTruthEntity == other.groundTruthEntity;
  }

  override def hashCode() : Int = {
    return entity + mention.hashCode + filePath.hashCode;
  }
}
