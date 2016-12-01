package index

import gnu.trove.map.hash.TObjectIntHashMap
import gnu.trove.set.hash.TIntHashSet

/**
 * Maps Wikipedia page titles to integer ids.
 */
class EntNameToIDIndex extends java.io.Serializable {

  private val INITIAL_SIZE = 4399390

  // Wikipedia titles not found in the index are disambiguation/list/category pages.
  val NOT_CANONICAL_TITLE = -1

  private val map = new TObjectIntHashMap[String](INITIAL_SIZE * 4 / 3 + 1)

  def load(path : String) = {
	System.err.println("Collecting disambiguation pages...")
	val disambiguationPages = new TIntHashSet(200000)
	
	val allDocsFile = "/media/hofmann-scratch/Octavian/entity_linking/marinah/wikipedia/disambiguation_pages"
	val allDocsLines = scala.io.Source.fromFile(allDocsFile).getLines
	allDocsLines.foreach(line => {
	  val entID = line.split("\t")(0).toInt
	  disambiguationPages.add(entID)
	})	
	System.err.println("Done collecting disambiguation pages.")
	
    
    System.err.println("Loading entNameToID index... " + path);
	val lines = scala.io.Source.fromFile(path).getLines

	var nr_disambig_removed = 0
	lines.foreach(line => {
	  val elements = line.split("\t", 3)
	  val entID = elements(1).toInt
	  if (!disambiguationPages.contains(entID)) {
	    map.put(elements(0).trim(), entID)
	  } else {
	    nr_disambig_removed += 1
	  }
	})
	  
	System.err.println("Loaded entNameToID index. Size = " + map.size + " . Nr disambig removed = " + nr_disambig_removed)
  }

  def getTitleId(title : String) : Int = {
    if (map.containsKey(title)) {
      return map.get(title)
    }
    return NOT_CANONICAL_TITLE
  }

  def containsKey(title : String) : Boolean = {
    map.containsKey(title)
  }

}
