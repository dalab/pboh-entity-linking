package utils

import java.io.PrintWriter;
import java.io.Serializable;

import gnu.trove.iterator.TLongDoubleIterator;
import gnu.trove.map.hash.TLongDoubleHashMap;
import scala.collection.JavaConversions._

/*
 *  Memory efficient implementation of HashMap[(Integer,Integer), Double]
 *  used to store the lambdas params.
 */
class OptimizedLambdasMap(maxsize : Int) extends Serializable {

  val map = new TLongDoubleHashMap(maxsize);
	

  def getKey(x1 : Int, x2 : Int) : Long = {
    return (Math.min(x1,x2).toLong  << 31) + Math.max(x1,x2).toLong;
  }

  // Public methods:
  def clear() {
    map.clear();
  }

  def contains(x1 : Int, x2 : Int) : Boolean = {
    val key = getKey(x1,x2)
    return map.containsKey(key);
  }		

  def getSize() : Int = {
    return map.size();
  }

  def putLambda(x1 : Int, x2 : Int, value : Double) {
    val key = getKey(x1,x2);
    map.put(key, value);
  }
	
  def putLambdaKey(key : Long, value : Double) {
    map.put(key, value);
  }
  
  def getLambdaKey(key : Long) : Double = {
    return map.get(key);
  }

  def getOrElseZero(x1 : Int, x2 : Int) : Double = {
    val key = getKey(x1,x2);
    if (!map.containsKey(key)) return 0.0;
    return map.get(key);
  }	

  override def clone() : OptimizedLambdasMap = {
    val rez = new OptimizedLambdasMap(map.size());
    for (key <- map.keys()) {
      rez.map.put(key, map.get(key));
    }
    return rez;
  }

  def add(other : OptimizedLambdasMap) {
    for (key <- other.map.keys()) {
      if (map.contains(key)) {
        map.put(key, map.get(key) + other.map.get(key));
      } else {
        map.put(key, other.map.get(key));
      }
    }
  }

  def multiply(v : Double) {
    for (key <- map.keys()) {
      map.put(key, map.get(key) * v);
    }
  }


  def print(outputFile : String) {
    val writer = new PrintWriter(outputFile, "UTF-8");
    writer.println("\n\nLambdas format: E1\tE2\tscore\tstoredKey");
	
    for (k <- map.keys()) {
      val value = map.get(k);
      val e1 = (k >> 31).toInt;
      val e2 = (k & ((1 << 31) - 1)).toInt;
      writer.println(e1 + "\t" + e2 + "\t" + value + "\t" + k);
    }		
    writer.close();
  }
}
