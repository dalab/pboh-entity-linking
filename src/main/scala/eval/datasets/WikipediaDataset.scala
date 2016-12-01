package eval.datasets

import scala.collection.mutable.MutableList
import scala.io.Source
import index.AllIndexesBox
import context.StopWords
import context.TextSplittingInWords
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import gnu.trove.set.hash.TIntHashSet
import gnu.trove.map.hash.TIntObjectHashMap

object WikipediaDataset {

  def loadDataset(allIndexesBox : AllIndexesBox) : Array[(String, Array[(String,Int, Array[Int])])] = {
    // Wikipedia validation set
    val mentionEntitiesFile =  "/media/hofmann-scratch/Octavian/entity_linking/marinah/wikipedia/anchorsListFromEachWikiPage.txt_val"
    val contextFile = "/media/hofmann-scratch/other-data/Wikipedia/WikipediaPlainText/textFromAllWikipedia2014Feb.txt_one_doc_per_line_validation"
     
    val sw = new StopWords
    val wordFreqDict = allIndexesBox.wordFreqDict
    
    println("\nLoading Wikipedia Validation file from " + mentionEntitiesFile)
    
    val result = new ArrayBuffer[(String, Array[(String, Int, Array[Int])])]
    
    // Find the set of pages IDs in this validation set and retrieve their corresponding texts.
    val docIdsToMentionEnts = new TIntObjectHashMap[ArrayBuffer[(String,Int)]]
    
    val linesAnchors = Source.fromFile(mentionEntitiesFile).getLines()
    while (linesAnchors.hasNext) {
      val line = linesAnchors.next
      val tokens = line.split("\t")
      assert(tokens.length > 0, "line = " + line)
      val pageId = tokens(0).split(",").last.toInt
      
      if (tokens.length > 1) {
        val mentionEnts = new ArrayBuffer[(String, Int)]
        for (i <- 2 until tokens.length) {
          if (i % 2 == 0) {
            val mention = tokens(i-1).toLowerCase().trim()
            val entity = tokens(i).toInt
            if (!allIndexesBox.entIDToNameIndex.containsKey(entity)) {
              System.err.println("GROUND TRUTH ENT NOT IN EntNameToIDIndex  e = " + entity +
                  " ; m = " + mention +  " -- pageId = " + pageId)
            }            
            mentionEnts += ((mention, entity))
          }
        }
        assert(mentionEnts.size > 0, " Doc ID = " + pageId)
        docIdsToMentionEnts.put(pageId, mentionEnts)
      }        
    }
    
    var numMentions = 0
    var numMentionsWithNoContext = 0
    
    
    val linesPagesText = Source.fromFile(contextFile).getLines()
    while (linesPagesText.hasNext) {
      val line = linesPagesText.next
      if (line.startsWith("<doc id=") && line.contains("\">##\t\t\t")) {
        val docID = line.substring(
          line.indexOf(" id=\"") + " id=\"".length(), 
          line.indexOf("\" ", line.indexOf(" id=\"") + " id=\"".length())).toInt
        
        if (docIdsToMentionEnts.containsKey(docID)) {
          val text = line.substring(line.indexOf("\">##\t\t\t") + "\">##\t\t\t".length())
          val textWordsIds = TextSplittingInWords.stringToListOfWordIDs(text, sw, wordFreqDict)
          
          val listMentionsAndEnts = new ArrayBuffer[(String, Int, Array[Int])]
          
          val mentionEnts = docIdsToMentionEnts.get(docID)
          assert(mentionEnts.length > 0, " docID = " + docID)
          for (x <- mentionEnts) {
            val mention = x._1 
            val entity = x._2 
            val context = TextSplittingInWords.getContextWords(mention, textWordsIds, sw, allIndexesBox.wordFreqDict)
            if (context.length == 0) {
              numMentionsWithNoContext += 1
            }
            
            listMentionsAndEnts += ((mention, entity, context))
          }
          numMentions += listMentionsAndEnts.size
          result += (("" + docID, listMentionsAndEnts.toArray))
        }
      }
    }
     
    println("Done loading Wikipedia Validation set. Num mentions = " + numMentions +
        " num mentions with no context = " + numMentionsWithNoContext + " ; num docs = " + result.size)
    result.toArray
  }  
}