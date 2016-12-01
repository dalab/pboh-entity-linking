package index.index_builder

import java.io.FileInputStream
import java.util.zip.GZIPInputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import index.AllIndexesBox

object FreebaseIndexWrapper {
  private val allIndexes = new AllIndexesBox("freebaseToWikiIDIndex,entNameToIDIndex,redirectIndex")
  
  def getAllIndexesBox : AllIndexesBox = allIndexes
}

/*
 * Creates from the Freebase corpus found at
 * https://developers.google.com/freebase/data
 * an index with lines of form:
 * relationName,relId \t [freebaseID \t wikipediaID \t]*
 * 
 * To run scala -J-Xmx60g -J-Xms60g -classpath "lib/ *" EL_LBP_Spark createFreebaseAnchorIndex /path/to/the/gz/archive /path/to/output/file
 * 
 * scala -J-Xmx60g -J-Xms60g -classpath "lib/ *" EL_LBP_Spark createFreebaseAnchorIndex ./freebase-rdf-latest.gz ./anchorsListFromFrebaseRelations.txt
 */
object CreateFreebaseAnchorIndex {

  def run(args: Array[String]) : Unit = {
    if (args.length < 2) {
      System.err.println("Usage: input_file output_file")
      System.exit(1)
    }
    val gzis = new GZIPInputStream(new FileInputStream(args(0)))
    val reader = new InputStreamReader(gzis);
    val in = new BufferedReader(reader);
	
    val allIndexesBox = FreebaseIndexWrapper.getAllIndexesBox
    val fw = new java.io.FileWriter(args(1))
    
    var pageID = 30000000 - 1

    var line = in.readLine()
    while (line != null) {
      if (line.startsWith("<http://rdf.freebase.com/ns/m.") && 
          line.indexOf("<http://rdf.freebase.com/ns/m.",10) > 0) {
        val tokens = line.split("\\s+")
        val relationString = tokens(1)
        
        var freebaseId0 = tokens(0).substring(tokens(0).indexOf("/ns/m.") + "/ns/m.".length())
        freebaseId0 = freebaseId0.substring(0, freebaseId0.length() - 1)
        var ent0 = -1
        if (allIndexesBox.freebaseToWikiIDIndex.containsKey(freebaseId0)) {
          ent0 = allIndexesBox.freebaseToWikiIDIndex.get(freebaseId0)
        }
        
        var freebaseId2 = tokens(2).substring(tokens(2).indexOf("/ns/m.") + "/ns/m.".length())
        freebaseId2 = freebaseId2.substring(0, freebaseId2.length() - 1)
        var ent2 = -1
        if (allIndexesBox.freebaseToWikiIDIndex.containsKey(freebaseId2)) {
          ent2 = allIndexesBox.freebaseToWikiIDIndex.get(freebaseId2)
        }
        
        if (ent0 > 0 && ent2 > 0) {
          pageID += 1 
          val docLineToWrite = relationString + "," + pageID + "\t" + freebaseId0 + "\t" + ent0 + "\t"  + freebaseId2 + "\t" + ent2
          fw.write(docLineToWrite  + "\n")
        }
      }
      line = in.readLine()
    }
    
    println("GATA . Total num : " + (pageID - 30000000))   
    
    fw.close()
  }
}