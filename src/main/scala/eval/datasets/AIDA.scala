package eval.datasets
import scala.collection.mutable.MutableList
import scala.io.Source
import index.AllIndexesBox
import context.StopWords
import context.TextSplittingInWords
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer

// Taken from https://www.mpi-inf.mpg.de/departments/databases-and-information-systems/research/yago-naga/aida/downloads/
object AIDA {
  def loadDataset(testA : Boolean, allIndexesBox : AllIndexesBox) : Array[(String, Array[(String,Int, Array[Int])])] = {
    val file =  "/media/hofmann-scratch/Octavian/entity_linking/marinah/AIDA/testa_testb_aggregate"
     
    val sw = new StopWords
    
    println("\nLoading AIDA " + (if (testA) "test A" else "test B") + " file from " + file)
    

    val result = new ArrayBuffer[(String, Array[(String, Int, Array[Int])])]
    val lines = Source.fromFile(file).getLines()
    var numMentions = 0
    
    var docName = "";
    var docWordsMentionsAndEntities = new ArrayBuffer[(String, (String, Int))] // List[(word, (mention, entity))]
    
    var insideTestSet = false
    
    var numMentionsWithNoContext = 0
    
    def finishCurrentDoc() = {
      if (docName != "" && ((testA && docName.contains("testa")) || (!testA && docName.contains("testb")))) {
        val listMentionsAndEnts = new ArrayBuffer[(String, Int, Array[Int])]
        
        println(" CURRENT DOC TEXT = " + docWordsMentionsAndEntities.map(_._1).mkString(" ") + "\n")
        
        val textWordsIds = 
          TextSplittingInWords.stringToListOfWordIDs(docWordsMentionsAndEntities.map(_._1).mkString(" "), sw, allIndexesBox.wordFreqDict)
          
        var indexInTextWordsIds = 0
        for (x <- docWordsMentionsAndEntities) {
          val word = x._1 
          val mentEnt = x._2 
          if (mentEnt != null) {
            val mention = mentEnt._1 
            val entity = mentEnt._2 
            val context = TextSplittingInWords.getContextWords(indexInTextWordsIds, mention, textWordsIds, sw, allIndexesBox.wordFreqDict)
            if (context.length == 0) {
              numMentionsWithNoContext += 1
            }
            listMentionsAndEnts += ((mention, entity, context))
            println (" ==== c === :: m = " + mention + " ; c = " + context.map(c => allIndexesBox.wordFreqDict.wordIDtoString.get(c)).mkString(" "))
          }
          indexInTextWordsIds += TextSplittingInWords.stringToListOfWordIDs(word, sw, allIndexesBox.wordFreqDict).size
        }
        numMentions += listMentionsAndEnts.length
        result += ((docName, listMentionsAndEnts.toArray))
      }      
    }
    
    while(lines.hasNext) {
      val line = lines.next
      if (line.contains("-DOCSTART-")) {
        finishCurrentDoc
        docName = line.substring(line.indexOf("(") + 1, line.length() - 1)
        insideTestSet = ((testA && docName.contains("testa")) || (!testA && docName.contains("testb")))
        docWordsMentionsAndEntities = new ArrayBuffer[(String, (String, Int))]
      } else if (insideTestSet && line.size > 0) {
        if (line.contains("\tB\t") && !line.contains("-NME-")) {
          val tokens = line.split("\t")
          assert(tokens.size >= 6, line)
          val mention = tokens(2).toLowerCase().trim()
          val entityString = tokens(4).substring(tokens(4).lastIndexOf("/wiki/") + "/wiki/".length())
          val entity = allIndexesBox.entNameToIDIndex.getTitleId(allIndexesBox.redirectIndex.getCanonicalURL(entityString))

          if (entity != tokens(5).toInt) { // change to ASSERT
            println( "EEEEEEEEROR : ent = " + entity + " gtruth = " + tokens(5) + " m = " + mention + " entityString = " + entityString)
          }
          
          val word = tokens(0)
          
          if (entity == allIndexesBox.entNameToIDIndex.NOT_CANONICAL_TITLE || mention.length() == 0) {
            println("GROUND TRUTH ENT NOT IN EntNameToIDIndex  e = " + entityString +
                " --> " + allIndexesBox.redirectIndex.getCanonicalURL(entityString) + 
                "; entId = " + entity + "; m = " + mention + " -- doc = " + docName)
                
            docWordsMentionsAndEntities += ((word, null))
          } else {
            docWordsMentionsAndEntities += ((word, (mention, entity)))
          }      
          
        } else { // Just one word
          val word = line.split("\t")(0)
          docWordsMentionsAndEntities += ((word, null))
        }
      }
    }
    finishCurrentDoc()
    
    println("Done loading AIDA. Num mentions = " + numMentions +
        "; num Mentions With No Context = " + numMentionsWithNoContext + " ; num docs = " + result.size)
    result.toArray
  }  
  
}