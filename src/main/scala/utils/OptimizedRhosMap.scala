package utils

import java.io.Serializable
import gnu.trove.map.hash.TObjectDoubleHashMap
import scala.collection.JavaConversions._
import java.io.PrintWriter

/*
 *  Memory efficient implementation of HashMap[String, Double]
 *  used to store the rhos params.
 */
class OptimizedRhosMap(maxsize : Int) extends Serializable {

  val map = new TObjectDoubleHashMap[String](maxsize) 

  private def getPartialRhoKey(mention : String, entity : Int) : String = {
    val key = "" + entity + "\t" + mention.replace(' ', '_');
    if (key.getBytes("UTF-8").length < 240) {
      return key;
    }
    
    System.err.println("Key too long : " + key);
    if (key.length() == key.getBytes("UTF-8").length) {
      return key.substring(0, 239);
    }
    return key.substring(0, 30);
  } 

  // Public methods:
  def clear() {
    map.clear();
  }
	
  def getSize() : Int = {
    return map.size();
  }

  def contains(mention : String, entity : Int) : Boolean = {
    val key = getPartialRhoKey(mention, entity)
    return map.containsKey(key)
  }

  def getRho(mention : String, entity : Int) : Double = {
    val key = getPartialRhoKey(mention, entity)
    var res = 0.0
    if (map.containsKey(key)) {
      res = map.get(key);
    }
    return res;
  }

  def putRho(mention : String, entity : Int, v : Double) {
    val key = getPartialRhoKey(mention, entity)
    map.put(key, v)
  }   

  def putRhoKey(key : String, v : Double) {
    map.put(key, v);
  }
  
  def getRhoKey(key : String) : Double = {
    return map.get(key)
  }
  
  override def clone() : OptimizedRhosMap = {
    val rez = new OptimizedRhosMap(map.size())
    for (key <- map.keySet()) {
      rez.map.put(key, map.get(key));
    }
    return rez;
  }
  
  def add(other : OptimizedRhosMap) = {
    for (key <- other.map.keySet()) {
      if (map.contains(key)) {
        map.put(key, map.get(key) + other.map.get(key));
      } else {
        map.put(key, other.map.get(key));
      }
    }
  }
  
  def multiply(v : Double) = {
    for (key <- map.keySet()) {
      map.put(key, map.get(key) * v);
    }
  }

  def print(outputFile: String) =  {
    val writer = new PrintWriter(outputFile, "UTF-8")
    writer.println("\n\nRhos format: mention\tentity\tscore");
    for (k <- map.keySet()) {
      val value = map.get(k);
      writer.println(k + "\t" + value);
    }
    writer.close();
  }
}
