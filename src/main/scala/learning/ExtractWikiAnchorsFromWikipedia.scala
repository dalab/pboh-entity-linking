package learning

import index.AllIndexesBox
import scala.io.Source
import scala.collection.mutable.HashMap
import utils.Normalizer


/* Extracts WikipediaAnchors from an input text extracted with 
 * http://medialab.di.unipi.it/wiki/Wikipedia_Extractor using the -l option.
 * 
 * To run:
 * scala -J-Xmx50g -J-Xms50g -classpath "lib/ *"
 * EL_LBP_Spark extractWikipAnchors
 * /mnt/cw12/other-data/Wikipedia/WikipediaPlainText/textWithAnchorsFromAllWikipedia2014Feb.txt > output_file
 */

object ExtractWikiAnchorsFromWikipedia {

  object AllIndexesWrapper {
    private val indexesToLoad =
      "entNameToIDIndex,redirectIndex"
    private val allIndexes = new AllIndexesBox(indexesToLoad)
    
    def getAllIndexesBox : AllIndexesBox = allIndexes
  }

  def run(args : Array[String]) = {
    val allIndexesBox = AllIndexesWrapper.getAllIndexesBox
    
    val dataPath = args(0)
    val lines = Source.fromFile(dataPath).getLines
    var curPageId = -1
    var curPageTitle = ""
    
    var numGoodLinks = 0
    var numErrorLinks = 0
    var docsProcessed = 0
    
    var sb = new StringBuffer
    
    for (line <- lines) {
      if (line.startsWith("<doc ")) {
        val ct = "http://en.wikipedia.org/wiki?curid="
        curPageId = Integer.parseInt(
            line.substring(line.indexOf(ct) + ct.length, line.indexOf("\" title=")))
            
        val offset = line.indexOf("title=\"") + "title=\"".length
        curPageTitle = line.substring(offset, line.indexOf("\">", offset))
        curPageTitle = Normalizer.processTargetLink(curPageTitle)
        
        val x = allIndexesBox.entNameToIDIndex.getTitleId(curPageTitle)
        if (x < 0 || x != curPageId) {
          curPageId = -1
        }
      } else if (curPageId > 0) {
        if (line.startsWith("</doc>")) {
          docsProcessed += 1
          var out = new StringBuffer
          out.append(curPageTitle + "," + curPageId)
          
          var s = sb.toString()
          var t = s.indexOf("<a href=")
          while (t >= 0) {
            t += "<a href=\"".length()
            if (s.indexOf("\">",t) > 0 && s.indexOf("</a>", t) > 0) {
              var entName = s.substring(t, s.indexOf("\">",t))
              if (entName.startsWith("wikt:")) {
                entName = entName.substring("wikt:".length())
              }
              entName = Normalizer.processTargetLink(entName)
              entName = allIndexesBox.redirectIndex.getCanonicalURL(entName)
            
              val entId = allIndexesBox.entNameToIDIndex.getTitleId(entName)
              
              t = s.indexOf("\">",t) + "\">".length()
            
              val mention = s.substring(t, s.indexOf("</a>", t))
              
              if (entId < 0) {
                if (!entName.contains("#") && !entName.contains("List")) {
                  //System.err.println("Missing ent from NameToID index : " + entName + " ---- " + entId)
                  numErrorLinks += 1
                }
              } else {
                out.append("\t" + mention + "\t" + entId)
                numGoodLinks += 1
              }
            }
            t = s.indexOf("<a href=", t)
          }
          
          println(out.toString())
          sb = new StringBuffer
          
          if (docsProcessed % 10000 == 0) {
            System.err.println("docs : " + docsProcessed + " ; good links = " + numGoodLinks + " ; bad links = " +  numErrorLinks)
          }
        } else {
          sb.append(line + " ")
        }
      }
    }
  }
}