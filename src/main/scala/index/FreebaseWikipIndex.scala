package index;

import utils.Utils
import gnu.trove.map.hash.TObjectIntHashMap
import gnu.trove.map.hash.THashMap
import gnu.trove.set.hash.THashSet
import scala.collection.JavaConversions._

// Loads a Freebase - Wikipedia map and also enhances the Wikipedia redirects index.
class FreebaseWikipIndex extends java.io.Serializable {

  private val INITIAL_SIZE = 3999390;

  private val map = new TObjectIntHashMap[String](INITIAL_SIZE * 4 / 3 + 1)
  
  def load(
    freebaseWikiMapFile : String,
    redirectIndex : RedirectPagesIndex,
    entNameToIDIndex : EntNameToIDIndex) = {
		
	  // Set size to sustain all elements with a load factor of 0.75
	  System.err.println("Loading freebaseWiki map ... " + freebaseWikiMapFile);		

	  val lines = scala.io.Source.fromFile(freebaseWikiMapFile).getLines
	  
	  val freebaseAllWikisMap = new THashMap[String, THashSet[String]]();

	  lines.foreach(line => {
		  val elements = line.split("\t");
		  val freebaseId = elements(0).substring(elements(0).indexOf("m.") + 2);
		  val wikipEntity = Utils.extractWikipURL(elements(2));
		  if (!freebaseAllWikisMap.containsKey(freebaseId)) {
			  freebaseAllWikisMap.put(freebaseId, new THashSet[String]());
		  }
		  freebaseAllWikisMap.get(freebaseId).add(wikipEntity);
	  })

	  for (e <- freebaseAllWikisMap.entrySet()) {
		  // Multiple urls can be canonicals because all except one are disambiguation or list
		  // pages in the entNameToIDIndex. We select the one that appears the most number of
		  // times as the correct one.
		  val wikiCanonicals = new THashMap[String, Integer]();

		  for (w <- e.getValue()) {
			  val canonical = redirectIndex.getCanonicalURL(w);
			  if (entNameToIDIndex.containsKey(canonical)) {
				  if (!wikiCanonicals.containsKey(canonical)) {
					  wikiCanonicals.put(canonical, 0);
				  }
				  wikiCanonicals.put(canonical, wikiCanonicals.get(canonical) + 1);
			  }
		  }

		  // Fill redirects index with more data.
		  if (wikiCanonicals.size() > 0) {
			  var maxCount = 0;
			  var wikiCanonical = "";

			  for (can <- wikiCanonicals.entrySet()) {
				  if (can.getValue() > maxCount) {
					  maxCount = can.getValue();
					  wikiCanonical = can.getKey();
				  }
			  }

			  map.put(e.getKey(), entNameToIDIndex.getTitleId(wikiCanonical));
			  for (w <- e.getValue()) {
				  redirectIndex.put(w, wikiCanonical); // How many conflicts we have here ?????
			  }
		  }	  
	  }

	  System.err.println("Loaded freebaseWiki map. Size = " + map.size());
  }
  
  def containsKey(key : String) : Boolean = {
    map.containsKey(key)
  }
  
  def get(key : String) : Int = {
    map.get(key)
  }
  
}
