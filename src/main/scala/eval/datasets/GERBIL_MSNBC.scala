package eval.datasets

import scala.collection.mutable.MutableList
import scala.io.Source
import index.AllIndexesBox
import context.StopWords
import context.TextSplittingInWords
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import java.io.File

// Load data from here : http://webdocs.cs.ualberta.ca/~denilson/data/deos14_ualberta_experiments.tgz
object GERBIL_MSNBC {
      
  def loadDataset(name : String, allIndexesBox : AllIndexesBox) : Array[(String, Array[(String,Int, Array[Int])])] = {
    val baseDir =  "/media/hofmann-scratch/Octavian/entity_linking/marinah/gerbil_msnbc/"
    val rootDirMentions = baseDir + "Problems/"
    val rootDirContext = baseDir + "RawTextsSimpleChars_utf8/"
     
    val sw = new StopWords
    
    println("\nLoading " + name + " from directory:" + rootDirContext)
    
    val files = new File(rootDirContext).list()
    
    var globalnumMentions = 0
    var numMentionsWithNoContext = 0

    val result = new ArrayBuffer[(String, Array[(String, Int, Array[Int])])]
    
    for (fullfileName <- files) {
      if (fullfileName.endsWith(".txt")) {
        val fileName = fullfileName.substring(Math.max(0, fullfileName.lastIndexOf("/")))
        
        var text = Source.fromFile(rootDirContext + fileName).getLines().mkString(" ").replace('’', '\'')
        val textWordsIds : List[Int] = TextSplittingInWords.stringToListOfWordIDs(text, sw, allIndexesBox.wordFreqDict)
        val listMentionsAndEnts = new ArrayBuffer[(String, Int, Array[Int])]
        
        val groundTruthLines = Source.fromFile(rootDirMentions + fileName).getLines()

        var entityString = "";
        var mention = "";
        var entity = -1
        var offset = -1
        
        while (groundTruthLines.hasNext) {
          val line = groundTruthLines.next.replaceAll("&amp;", "&")
          if (line.contains("</ReferenceInstance>")) {

            if (mention.length() > 0 && entityString != "NIL" && entityString.length() > 0) {
              val context = TextSplittingInWords.getContextWords(mention, textWordsIds, sw, allIndexesBox.wordFreqDict)
              // I also include ground truth entities that I am not able to find in my indexes.
              listMentionsAndEnts += ((mention.toLowerCase(), entity, context)) ///////////////// ???????????????????????????????? toLowerCase ????
              if (context.length == 0) {
                numMentionsWithNoContext += 1
              }
              globalnumMentions += 1
            }              
            
          } else if (line.contains("<Offset>")) {
            val l = groundTruthLines.next.replaceAll("&amp;", "&")
            offset = l.toInt
      //      assert(text.substring(offset, offset + mention.length()).toLowerCase().trim() == mention.toLowerCase().trim(),
        //        " filename = " + fileName + " offset = " + offset + " --> " + text.substring(offset, offset + mention.length()))
          } else if (line.contains("<SurfaceForm>")) {
            mention  = groundTruthLines.next.replaceAll("&amp;", "&") // ??????????????? .toLowerCase().trim()
          } else if (line.contains("<Annotation>")) {
            entityString = groundTruthLines.next.replaceAll("&amp;", "&")
            entity = allIndexesBox.entNameToIDIndex.getTitleId(allIndexesBox.redirectIndex.getCanonicalURL(entityString))
          }
        }

        if (listMentionsAndEnts.length > 0) {
          result += ((fileName, listMentionsAndEnts.toArray))
        }        
      }
    }
    
    println("Done loading " + name + ". Num mentions = " + globalnumMentions +
        "; num Mentions With No Context = " + numMentionsWithNoContext + " ; num docs = " + result.size)
    result.toArray
  }  
  
}