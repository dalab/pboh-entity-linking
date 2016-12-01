package index

import gnu.trove.map.hash.TIntObjectHashMap
import gnu.trove.set.hash.TIntHashSet

class EntIDToNameIndex extends java.io.Serializable {

  private val INITIAL_SIZE = 4399390
  
  private val map = new TIntObjectHashMap[String](INITIAL_SIZE * 4 / 3 + 1)

  // Wikipedia titles not found in the index are disambiguation/list/category pages.
  val  NOT_CANONICAL_TITLE = -1

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
	
    
    System.err.println("Loading entIDToName index... " + path);
	val lines = scala.io.Source.fromFile(path).getLines
	var nr_disambig_removed = 0

	lines.foreach(line => {
	  val elements = line.split("\t", 3)
	  val entID = elements(1).toInt
	  if (!disambiguationPages.contains(entID)) {
	    map.put(entID, elements(0).trim())
	  } else {
	    nr_disambig_removed += 1
	  }
	})

	System.err.println("Loaded entIDToName index. Size = " + map.size + " . Nr disambig removed = " + nr_disambig_removed)
  }

  def get(entId : Int) : String = {
    if (map.containsKey(entId)) {
      return map.get(entId)
    }
    return "ENT_NOT_IN_ID_TO_NAME_INDEX"
  }

  def containsKey(entId : Int) : Boolean = {
    map.containsKey(entId)
  }
  
}
