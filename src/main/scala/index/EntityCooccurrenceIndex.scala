package index

import gnu.trove.list.array.TIntArrayList
import gnu.trove.map.hash.TIntObjectHashMap
import scala.collection.mutable.ArrayBuffer
import gnu.trove.set.hash.TLongHashSet
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import gnu.trove.impl.hash.TIntIntHash
import gnu.trove.map.hash.TIntIntHashMap
import gnu.trove.map.hash.TIntDoubleHashMap
import gnu.trove.set.hash.TIntHashSet

/**
 * Stores a mapping of (entity1, list of documents that link to entity1).
 * Can be used to compute semantic relatedness between two entities.
 * 
 * The file loaded should have each line of the form:
 * Wikipedia or Non-Wikipedia page source:
 *  "pageTitle,pageId \t [mention \t entityId]*" 
 *  
 *  for Wikipedia pages, the pageID is the same as the Wikipedia ID.
 */
class EntityCooccurrenceIndex(index_name : String) extends java.io.Serializable {
  
  private val INITIAL_SIZE = 4000000

  private val map = new TIntObjectHashMap[TIntArrayList](INITIAL_SIZE * 4 / 3 + 1)
  
  private val numEntsPerDoc = new TIntIntHashMap(INITIAL_SIZE * 4 / 3 + 1)

  private val popularity = new TIntDoubleHashMap(INITIAL_SIZE) // per entity
  
  private val cocit_e_e = new TIntDoubleHashMap
  
  private var numTotalLinks : Long = 1
  private var numTotalPairs : Long = 1
	
  /*
   *val numPairs : Long = 2132603507
    val numLinks : Long = 66886284
   */
  val numDistinctPairs : Long = 884522620
  

  def insert(docId : Int, entityId : Int) {
    if (!map.containsKey(entityId)) {
      map.put(entityId, new TIntArrayList());
    }
    val value = map.get(entityId);
    value.add(docId);
    map.put(entityId, value);
  }

  def computeNumDistinctPairs(path : String, wikipediaCorpus : Boolean) = {
    System.err.println("Counting pairwise stats from " + path + "...");
    val conf = new SparkConf().setAppName("EL LBP SPARK Learning few params " + Math.random())
    conf.set("spark.cores.max", "112")
    conf.set("spark.executor.memory", "100g")
    conf.set("spark.akka.frameSize", "1000")
    conf.set("spark.shuffle.file.buffer.kb", "1000")
    conf.set("spark.driver.maxResultSize", "100g")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    conf.set("spark.storage.memoryFraction", "1")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.core.connection.ack.wait.time10out", "600")
    
    val sc = new SparkContext(conf)
    
    val finalSet = sc.textFile(path, 112).mapPartitions(lines => {
      val set = new TLongHashSet

      for (line <- lines) {
        val tokens = line.split("\t");
        val pageTokens = tokens(0).split(",");
        val docId = Integer.parseInt(pageTokens(pageTokens.length - 1));
        val docTitle = tokens(0).substring(0, tokens(0).lastIndexOf(','));
        val entsArray = new ArrayBuffer[Int]

        // Include docId in the indexes of the pages of docId.
        if (wikipediaCorpus) {
          entsArray += docId
        }
      
        if (tokens.length >= 3) {
          for (i <- 2 until tokens.length by 2) {
            val entityId = Integer.parseInt(tokens(i));
            entsArray += entityId
          }
        }
      
        for (i <- 0 until entsArray.length) {
          for (j <- i + 1 until entsArray.length) {
            val e1 = Math.min(entsArray(i), entsArray(j))
            val e2 = Math.max(entsArray(i), entsArray(j))
            set.add((e1.toLong << 32) + e2.toLong)
          }
        }
      }
      set.toArray().toList.toIterator
      
    }).distinct

    System.err.println("Done counting. Num distinct pairs = " + finalSet.count);
  }
  
  def load(path : String, wikipediaCorpus : Boolean) = {
    System.err.println("Loading entity co-occurrence index from " + path + "...");

    val lines = scala.io.Source.fromFile(path).getLines

    val allEnts = new TIntHashSet(INITIAL_SIZE)
    
    lines.foreach(line => {
      val tokens = line.split("\t");
      val pageTokens = tokens(0).split(",");
      val docId = Integer.parseInt(pageTokens(pageTokens.length - 1));
      val docTitle = tokens(0).substring(0, tokens(0).lastIndexOf(','));

      // Include docId in the indexes of the pages of docId.
      if (wikipediaCorpus) {
        insert(docId,docId);
      }

      if (tokens.length >= 3) {
        var numDocLinks : Long = 0;
        var ents_with_freqs_per_doc = new TIntIntHashMap
        
        for (i <- 2 until tokens.length by 2) {
          val entityId = Integer.parseInt(tokens(i));
          numDocLinks += 1;
          insert(docId, entityId);
          allEnts.add(entityId)
          
          if (!ents_with_freqs_per_doc.containsKey(entityId)) {
            ents_with_freqs_per_doc.put(entityId, 0)
          }
          ents_with_freqs_per_doc.put(entityId, ents_with_freqs_per_doc.get(entityId) + 1)
        }
        
        numEntsPerDoc.put(docId, numDocLinks.toInt)
        numTotalLinks += numDocLinks;
        numTotalPairs += numDocLinks * (numDocLinks - 1);
        
        for (e <- ents_with_freqs_per_doc.keys()) {
          val freq = ents_with_freqs_per_doc.get(e)
          if (freq >= 2) {
            if (!cocit_e_e.containsKey(e)) {
              cocit_e_e.put(e, 0.0)
            }
            cocit_e_e.put(e, cocit_e_e.get(e) + freq * (freq - 1))
          }
        }
      }
    })
    
    for (e <- allEnts.toArray()) {
      var sum = 0.0
      for (doc <- map.get(e).toArray()) {
        if (numEntsPerDoc.containsKey(doc)) {
          sum += numEntsPerDoc.get(doc) - 1
        }
      }
      popularity.put(e, Math.max(1.0,sum))
    }

    System.err.println("Done loading entity links index. Num total links = " +
        numTotalLinks + "; num Total pairs = " + numTotalPairs + " ;size = " + map.size);
  }

  
  /*
   *  pop(e) = \sum_{e'} N(e,e') / N_pairs = \sum_{d contain e} (N_d - 1) / N_pairs
   */  
  def getPopularity(entity : Int) : Double = {
    if (!popularity.containsKey(entity)) {
      return 1.0
    }
    popularity.get(entity)
  }
  
  
  /*
   * Expensive operation.
   */
  def getCocitation(entity1 : Int, entity2 : Int, cocit_e_e_param : Double) : Double = {
    if (entity1 == entity2) {
      return cocit_e_e.get(entity1) + cocit_e_e_param * getPopularity(entity1)
    }
    
    if (!map.containsKey(entity1) || !map.containsKey(entity2)) {
      return 0
    }
    return intersectSize(map.get(entity1), map.get(entity2));
  }

  private def intersectSize(a : TIntArrayList, b : TIntArrayList) : Int = {
    if (a == null || b == null) {
      return 0;
    }
    
    var result = 0;
    var i = 0
    var j = 0
    while (i < a.size() && j < b.size()) {
      if (a.get(i) == b.get(j)) {
        result += 1
        i += 1
        j += 1
      } else if (a.get(i) < b.get(j)) {
        i += 1
      } else {
        j += 1
      }
    }
    return result;
  }

  def getTotalNumLinks() : Long = {
    return numTotalLinks;
  }

  def getTotalNumPairs() : Long = {
    return numTotalPairs;
  }
}