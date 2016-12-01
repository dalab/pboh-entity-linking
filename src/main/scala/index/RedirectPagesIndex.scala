package index;

import gnu.trove.map.hash.THashMap;
import scala.collection.JavaConversions._

/**
 * Maps Wikipedia redirect pages to canonical page title.
 */
class RedirectPagesIndex extends java.io.Serializable{
  private val INITIAL_SIZE = 6423968;
	
  val map = new THashMap[String, String](INITIAL_SIZE * 4 / 3 + 1);

  
  def containsKey(key : String) : Boolean = {
    return map.containsKey(key);
  }

  def get(key : String) : String = {
    if (!map.containsKey(key)) {
      return "";
    }
    return map.get(key);
  }
  
  
  def put(fromTitle : String, toTitle : String) = {
    map.put(fromTitle, toTitle);
  }

  
  /*
   * Redirects the input to the canonical URL, or leaves it unchanged, in case it is not a redirect
   * URL.
   */
  def getCanonicalURL(input : String) : String = {
    val input_norm = input.trim().replace('_', ' ').capitalize;
    if (map.containsKey(input_norm)) {
      return map.get(input_norm);
    }
    return input_norm;
  }
	
  def load(path : String) = {
    System.err.println("Loading redirect index " + path + "...");

	val lines = scala.io.Source.fromFile(path).getLines

	lines.foreach(line => {
	  val elements = line.split("\t", 3)
	  val fromTitle = elements(0).trim();
	  val toTitle = elements(1).trim();
	  
	  map.put(fromTitle, toTitle);
	})
    
	System.err.println("Loaded redirect index. Size = " + map.size() );
  }
}
