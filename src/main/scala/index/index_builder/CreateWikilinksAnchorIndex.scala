package index.index_builder

import scala.collection.mutable.HashSet

/*
 * Creates from the Wikilinks corpus an index with lines of form:
 * pageName,pageId \t [mention \t entity \t]*
 * 
 * The input folder should contain the DECOMPRESSED data from here :
 * https://code.google.com/p/wiki-links/downloads/list
 * 
 * To run scala -J-Xmx60g -J-Xms60g -classpath "lib/ *" EL_LBP_Spark createWikilinksAnchorIndex /path/to/the/data/dir/ /path/to/output/file
 * 
 * scala -J-Xmx60g -J-Xms60g -classpath "lib/ *" EL_LBP_Spark createWikilinksAnchorIndex ./marinah/wikilinks/ ./marinah/wikipedia/anchorsListFromEachWikilinksDoc.txt
 */

object CreateWikilinksAnchorIndex {

  
  def run(args: Array[String]) : Unit = {
    if (args.length < 2) {
      System.err.println("Usage: input_file output_file")
      System.exit(1)
    }
    
    val allIndexesBox = AllIndexesWrapper.getAllIndexesBox
    val fw = new java.io.FileWriter(args(1))
    
    var pageID = 10000000 
    
    var listFiles = new java.io.File(args(0)).listFiles.filter(_.getName.startsWith("data-"))
    for (f <- listFiles) {
      val lines = scala.io.Source.fromFile(f.getAbsolutePath()).getLines
      
      var url = "";
      var docLineToWrite = ""
      var mentionsMap : HashSet[String] = null
      while (lines.hasNext) {
        val line = lines.next
        
        if (line.startsWith("URL\t")) {
          url = line.split("\t")(1)
          docLineToWrite = url + "," + pageID + "\t"
          mentionsMap = new HashSet[String]
          pageID += 1
        } else if (line.startsWith("MENTION\t")) {
          val v = line.split("\t")
          val mention = v(1)
          var entityString = v(3)
          
          // Conversion: Wiki url <-> Wiki id
          if (entityString.contains("wikipedia.org/wiki/")) {
            entityString = entityString.substring(entityString.indexOf("wikipedia.org/wiki/") + "wikipedia.org/wiki/".length())
            val normalizedTarget = utils.Normalizer.processTargetLink(entityString); //check if it exists
            val canonicalTarget = allIndexesBox.redirectIndex.getCanonicalURL(normalizedTarget);
            val entityId = allIndexesBox.entNameToIDIndex.getTitleId(canonicalTarget);
            if (entityId > 0 && !mentionsMap.contains(mention)) {
              docLineToWrite += mention + "\t" + entityId + "\t"
              mentionsMap += ((mention))
            }
          }
        } else {
          if (url != "" && docLineToWrite != "" && mentionsMap != null) {
            fw.write(docLineToWrite  + "\n")
          }
          url = ""
          docLineToWrite = ""
          mentionsMap = null
        }
      }
    }
    
    fw.close()
  }
}