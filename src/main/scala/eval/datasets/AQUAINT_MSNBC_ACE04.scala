package eval.datasets

import scala.collection.mutable.MutableList
import scala.io.Source
import index.AllIndexesBox
import context.StopWords
import context.TextSplittingInWords
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer

// Load data from here : http://webdocs.cs.ualberta.ca/~denilson/data/deos14_ualberta_experiments.tgz
object AQUAINT_MSNBC_ACE04 {
  val rootFilesMentions = collection.immutable.HashMap(
      "AQUAINT" -> "aquaint/aquaint.xml",
      "MSNBC" -> "msnbc/msnbc.xml",
      "ACE04" -> "ace2004/ace2004.xml")

  val rootDirsContexts = collection.immutable.HashMap(
      "AQUAINT" -> "aquaint/RawTexts/",
      "MSNBC" -> "msnbc/RawTexts/",
      "ACE04" -> "ace2004/RawTexts/")
      
  def loadDataset(name : String, allIndexesBox : AllIndexesBox) : Array[(String, Array[(String,Int, Array[Int])])] = {
    val baseDir =  "/media/hofmann-scratch/Octavian/entity_linking/marinah/deos14_ualberta_experiments/"
    val rootFileMentions = baseDir + rootFilesMentions.get(name).get
    val rootDirContext = baseDir + rootDirsContexts.get(name).get
     
    val sw = new StopWords
    
    println("\nLoading " + name + " from directory:" + rootDirContext)
    var numMentions = 0

    val result = new ArrayBuffer[(String, Array[(String, Int, Array[Int])])]
    
    val groundTruthLines = Source.fromFile(rootFileMentions).getLines()
    
    var textWordsIds : List[Int] = null
    var fileName = "";
    var listMentionsAndEnts = new ArrayBuffer[(String, Int, Array[Int])]

    var entityString = "";
    var mention = "";        
    var entity = -1
    
    var numMentionsWithNoContext = 0
    
    while(groundTruthLines.hasNext) {
      val line = groundTruthLines.next.replaceAll("&amp;", "&")
      
      if (line.contains("<document docName=")) {
        // New document
        fileName = line.substring(line.indexOf("<document docName=\"") + "<document docName=\"".length(), line.indexOf("\">"))
        val text = Source.fromFile(rootDirContext + fileName).getLines().mkString(" ")
        textWordsIds = TextSplittingInWords.stringToListOfWordIDs(text, sw, allIndexesBox.wordFreqDict)
        listMentionsAndEnts = new ArrayBuffer[(String, Int, Array[Int])]
       
      } else if (line.contains("</document>")) {
        if (fileName.length() > 0 && listMentionsAndEnts.length > 0) {
          result += ((fileName, listMentionsAndEnts.toArray))
        }        
      } else if (line.contains("<annotation>")) {
        mention = ""
        entity = -1
        entityString = ""
      } else if (line.contains("<mention>")) {
        mention = line.substring(line.indexOf("<mention>") + "<mention>".length(), line.indexOf("</mention>"))
        mention = mention.toLowerCase().trim()
      } else if (line.contains("wikiName")) {
        // Ignore NIL entity.
        if (!line.contains("<wikiName>NIL</wikiName>") && !line.contains("<wikiName/>") && !line.contains("<wikiName></wikiName>")) {
          entityString = line.substring(line.indexOf("<wikiName>") + "<wikiName>".length(), line.indexOf("</wikiName>"))
          entity = allIndexesBox.entNameToIDIndex.getTitleId(allIndexesBox.redirectIndex.getCanonicalURL(entityString))

          if (entity == allIndexesBox.entNameToIDIndex.NOT_CANONICAL_TITLE) {
            System.err.println("GROUND TRUTH ENT NOT IN EntNameToIDIndex  e = " + entityString +
              " --> " + allIndexesBox.redirectIndex.getCanonicalURL(entityString) + 
              " entId = " + entity + " -- file = " + fileName)
          }        
        }
      } else if (line.contains("</annotation>")) {
        if (mention.length() > 0 && entityString != "NIL" && entityString.length() > 0) {
          val context = TextSplittingInWords.getContextWords(mention, textWordsIds, sw, allIndexesBox.wordFreqDict)
          // I also include ground truth entities that I am not able to find in my indexes.
          listMentionsAndEnts += ((mention.toLowerCase(), entity, context))
          if (context.length == 0) {
            numMentionsWithNoContext += 1
          }
          numMentions += 1
        }        
      }
    }
    
    println("Done loading " + name + ". Num mentions = " + numMentions +
        "; num Mentions With No Context = " + numMentionsWithNoContext + " ; num docs = " + result.size)
    result.toArray
  }  
  
}