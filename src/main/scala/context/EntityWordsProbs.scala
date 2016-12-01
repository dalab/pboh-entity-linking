package context
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import index.AllIndexesBox
import scala.collection.mutable.MutableList
import index.WordFreqDict
import scala.collection.mutable.HashMap
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable.HashSet
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import gnu.trove.set.hash.TIntHashSet
import scala.collection.JavaConversions._
import gnu.trove.map.hash.TObjectIntHashMap
import utils.Utils

/*
 * MASTER=spark://dco-node040.dco.ethz.ch:7077 
 * /mnt/cw12/Octavian/spark-1.1.1-bin-hadoop2.4/bin/spark-submit
 *  --class "EL_LBP_Spark" --jars lib/el_lbp_clueweb_list_words_2.10-1.0.jar,
 *  lib/trove-3.1a1.jar,lib/spymemcached-2.10.3.jar lib/el-lbp.jar EntityWordsProbs
 *   > >(tee out.log) 2> >(tee err.log >&2)
 * 
 */
object AllIndexes {
  private val indexesToLoad = "wordFreqDict,entIDToNameIndex"
  
  private val allIndexes = new AllIndexesBox(indexesToLoad)
  
  val wordFreqDict = allIndexes.wordFreqDict
  val entIDToNameIndex = allIndexes.entIDToNameIndex
}

object EntityWordsProbs {
  
  val K = 50 // window size over 2 (K at left and K at right of the mention)
  
  /*
   * lines contain a line with a list of (mention,entity)
   */
  private def extractWordEntityPairsFromOneDoc(
      entID : Int,
      lines : Iterable[String],
      sw : StopWords) : List[(Long, Int)] = {
    
    val wordFreqDict = AllIndexes.wordFreqDict
    
    var doc_string = ""
      
    // Vector (m,e) of hyperlinks in this doc.  
    var mentionEnts : TObjectIntHashMap[String] = null
    
    for (line <- lines) {
      if (line.startsWith("<doc id=") && line.contains("\">##\t\t\t")) {
        doc_string = line
      } else {
        val tokens = line.split("\t")
      
        if (tokens(0).contains(",")) {
          mentionEnts = new TObjectIntHashMap[String]
          for (i <- 2 until tokens.length) {
            if (i % 2 == 0) {
              mentionEnts.put(tokens(i-1), tokens(i).toInt)
            }
          }
        }
      } 
    }

    val rez = new MutableList[(Long, Int)]

    if (doc_string.length() > 0 && mentionEnts != null) {      
      // Build an index (first mention word -> mention)
      val firstWordMentionsHash = new HashMap[Int, MutableList[String]]
      for (m <- mentionEnts.keySet) {
        val m_words = TextSplittingInWords.stringToListOfWordIDs(m, sw, wordFreqDict)
        if (m_words.size > 0) {
          val first_word_id = m_words(0)
          if (!firstWordMentionsHash.contains(first_word_id)) {
            firstWordMentionsHash.put(first_word_id, new MutableList[String])
          }
          firstWordMentionsHash.get(first_word_id).get += m            
        }
      } 

      // Actual text of the page
      val text = doc_string.substring(doc_string.indexOf("\">##\t\t\t") + "\">##\t\t\t".length())
      
      val doc_words_ids = TextSplittingInWords.stringToListOfWordIDs(text, sw, wordFreqDict)
      
      // Do the actual pass through the text:      							   
      for (i <- 0 until doc_words_ids.size) {
        val w_id = doc_words_ids(i)
        if (firstWordMentionsHash.contains(w_id)) {
          // Find possible mentions that might start with this word
          val possibleMentions = firstWordMentionsHash.get(w_id).get
          for (m <- possibleMentions) {
            val words_m_ids = TextSplittingInWords.stringToListOfWordIDs(m, sw, wordFreqDict)

            // Verify if any such mention really starts at position i.
            var isMatch = true
            for (j <- 1 until words_m_ids.size) {
              if (i + j >= doc_words_ids.size) {
                isMatch = false
              } else if (words_m_ids(j) != doc_words_ids(i + j)) {
                isMatch = false
              }
            }
            
            // If it starts:
            if (isMatch) {
              val ent = mentionEnts.get(m)
              
              // Keep just distinct words in the window around this entity !!!!!!!!!!!!!!!!!
              val window_words = new TIntHashSet
              
              // Output pairs (w,e) where w is in the K window context of e.
              for (j <- i-1 to Math.max(0, i - K) by -1) {
                val w_j = doc_words_ids(j)
                window_words.add(w_j)
              }
              for (j <- (i + words_m_ids.size) until Math.min(i + words_m_ids.size + K - 1, doc_words_ids.size)) {
                val w_j = doc_words_ids(j)
                window_words.add(w_j)
              }
              
              for (w <- window_words.toArray()) {
                rez += ((Utils.compressTwoInts(w, ent), 1))
              }
            }              
          }
        }
      }
    }
    
    rez.toList
  }
  

  def pruneAndAdd(list : Iterable[(Int, Int)]) : (Long, List[(Int,Int)]) = {
    val wordFreqDict = AllIndexes.wordFreqDict

    def diff(t: (Int, Int)) = - (t._2 + 0.0) / wordFreqDict.wordToFreq.get(t._1)

    var total : Long = 0L
    for (x <- list) {
      total += x._2.toLong
    }
    
    (total, list.toList.filter(_._2 >= 2).sortBy(diff))
  }
  
  
  def getEntId(line : String) : Int = {
    // Either a line representing a doc string.
    if (line.startsWith("<doc id=") && line.contains("\">##\t\t\t")) {
      val docID = line.substring(
          line.indexOf(" id=\"") + " id=\"".length(), 
          line.indexOf("\" ", line.indexOf(" id=\"") + " id=\"".length())).toInt
          
      return docID
    } else {
      val tokens = line.split("\t")
      
      // Or a line representing a set of (mention,entity) pairs in an entity's page
      if (tokens(0).contains(",")) {
        val pageTokens = tokens(0).split(",")
        val docId = pageTokens(pageTokens.length - 1).toInt
        return docId
      }
    }
    -1
  }
  
  def run() : Unit = {
    Utils.unitTestCompressFull
    
    val input_mentions_per_doc = "/mnt/cw12/Octavian/el-lbp-marina/marinah/wikipedia/anchorsListFromEachWikiPage.txt"
    val input_docs = "/mnt/cw12/other-data/Wikipedia/WikipediaPlainText/textFromAllWikipedia2014Feb.txt_one_doc_per_line"
      
    val output = "/mnt/cw12/other-data/Wikipedia/WikipediaPlainText/textFromAllWikipedia2014Feb.txt_w_e_counts_dir"
    
    val conf = new SparkConf().setAppName("EL LBP SPARK " + Math.random())
    
    conf.set("spark.cores.max", "128")
    conf.set("spark.akka.frameSize", "1024")
    conf.set("spark.executor.memory", "90g") // don't increase this memory, or you'll get weird errors
    conf.set("spark.shuffle.file.buffer.kb", "1000")
    conf.set("spark.shuffle.consolidateFiles", "true")    
    conf.set("spark.reducer.maxMbInFlight", "1000")
 
    
    var sc = new SparkContext(conf)
    val sw = new StopWords
    val path = input_docs + "," + input_mentions_per_doc
  
    val rdd_1 = sc.textFile(path)
    	.map(line => (getEntId(line), line))
    	.persist(StorageLevel.MEMORY_AND_DISK)
    	.groupByKey
    	.flatMap(x => extractWordEntityPairsFromOneDoc(x._1, x._2, sw))
    	.reduceByKey(_ + _)
    	.map({case(x,count) => (Utils.decompressOneLong(x), count)})
    	.map({case((w,e),count) => (e, (w,count))})
    	.groupByKey
    	.map({case(e,list) => (e, pruneAndAdd(list))})
    	.filter({case (e,(total, list)) => list.size > 0})
    	.map({case(e,(total, list)) => 
    	  		AllIndexes.entIDToNameIndex.get(e) + "\t" + e + "\t" +
    			total + "\t" +
    			list.map(x => AllIndexes.wordFreqDict.wordIDtoString.get(x._1)  + ","+ x._2).mkString(" ")})
    	.saveAsTextFile(output)

    sc.stop()    
  }
}