package loopybeliefpropagation;

import md.Mention;
import gnu.trove.map.hash.TObjectDoubleHashMap

// Message = (mention from, mention to, entity)
class MessagesMap {
  
  private val map = new TObjectDoubleHashMap[Message]()
  
  private val neighborMessagesSum = new TObjectDoubleHashMap[(Mention, Int)] ;

  
  /**
   * Sums all the messages coming from the mentions in the list going to the source and entity, 
   * excluding one entity
   * @param source	All messages, except one, going to this source and entity are summed up.
   * @param entity All messages, except one, going to this source and entity are summed up.
   * @param exclude	The message coming from this mention is excluded
   * @param mentions All neighbor of the source mention (including itself).
   * 
   * @return	The computed sum
   */
  def sumNeighborMessages(
      source : Mention, 
      entity : Int, 
      exclude : Mention, 
      mentions : Array[Mention]) : Double = {
    
    val candidate = (source, entity);
    
    if (!neighborMessagesSum.containsKey(candidate)) {
      var sum = 0.0;
      for (from <- mentions) {
        if (!from.equals(source)) {
          sum += map.get(new Message(from, source, entity));
        }
      }
      neighborMessagesSum.put(candidate, sum);
    }
    
    var result = neighborMessagesSum.get(candidate);
    if (exclude != null) {
      result -= map.get(new Message(exclude, source, entity));
    }
    return result;
  }
  
  def containsMsg(key : Message) : Boolean = {
    map.containsKey(key)
  }
  
  def putMsg(key : Message, value : Double) = {
    map.put(key, value)
  }
  
  def get(key : Message) = {
    map.get(key)
  }
}
