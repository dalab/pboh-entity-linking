package index;

import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntLongHashMap;

import org.iq80.leveldb.Options
import org.fusesource.leveldbjni.JniDBFactory._
import java.io.File
import java.nio.ByteBuffer

/*
 * p(w|e) index
 */
class WordEntityProbsIndex extends java.io.Serializable  {

  private val INITIAL_SIZE = 2400000;
  
  val entToTotal = new TIntLongHashMap(INITIAL_SIZE);

  val entToNumNonzeroWordProbs = new TIntIntHashMap(INITIAL_SIZE);
  
  
//  private val entToWordFreq = Array.fill[TIntIntHashMap](600000)(new TIntIntHashMap(300)); // 600 K words in total

  private val options = new Options()
  options.createIfMissing(true)
  private val entToWordFreqDb = factory.open(new File("entToWordFreq.db"), options)

  def dbKey(k1: Int, k2: Int) = ByteBuffer.allocate(2 * 4).putInt(k1).putInt(k2).array()

  def toDbVal(v: Int) = ByteBuffer.allocate(4).putInt(v).array()

  def fromDbVal(v: Array[Byte]) = ByteBuffer.wrap(v).getInt()


  /*
   *  \sum_w p~ (w|e) = 1 + \sum_{w \in X} (\hat{p}(w|e) - \hat{p}(w))
   *   where X is the set of words for which we computed a p(w|e)
   */
  val sum_unnorm_p_tilda_w_e = new TIntDoubleHashMap(INITIAL_SIZE);
  
  var totalNumEnts = 0;
  
  /////////////////////////////////////////////////////////////////////////

  def putWordEntityFreq(word : Int, entity : Int, freq : Int) = {
//    entToWordFreq(word).put(entity, freq);
    entToWordFreqDb.put(dbKey(word, entity), toDbVal(freq))
  }
  
  def getWordEntityFreq(word : Int, entity : Int) : Int =  {
//    if (!entToWordFreq(word).containsKey(entity)) {
//      return 0;
//    }
//    return entToWordFreq(word).get(entity);

    val f = entToWordFreqDb.get(dbKey(word, entity))
    if (f == null) {
      return 0
    } else {
      return fromDbVal(f)
    }
  }
  
  def containsWordEntityFreq(word : Int, entity : Int) : Boolean = {
//    return entToWordFreq(word).containsKey(entity);
    entToWordFreqDb.get(dbKey(word, entity)) != null
  }

  def load(path : String, wordFreqDict : WordFreqDict) = {
    System.err.println("Loading word entity prob index p(w|e) from " + path + "...");
    val lines = scala.io.Source.fromFile(path).getLines

    var line_nr = 0;
    var nr_elems : Long = 0;
    
    while (lines.hasNext) {
      val line = lines.next
      line_nr += 1
      if (line_nr % 100000 == 0) {
        System.err.println(" ========== Loaded " + line_nr + " ;nr elems = " + nr_elems);
      }
      
      val tokens = line.split("\t");
      assert (tokens.length == 4, line)
      
      // tokens[0] = string of entity
      val ent = Integer.parseInt(tokens(1));
      
      val total = tokens(2).toLong;
      entToTotal.put(ent, total);

      var sum_unorm_p_tilda_w_e_local = 0.0;
      
      var numWordsPerEntity = 0
      
      val listWordsStr = tokens(3).split(" ");
      for (str <- listWordsStr) {
        val spl = str.split(",");
        val word = spl(0);
        val freq = spl(1).toInt;
        
        assert(freq >= 1)
        if (freq >= 3) { // Too slow to load if I don't do this.
          nr_elems += 1
          
          val word_id = wordFreqDict.wordStringToID.get(word);
          putWordEntityFreq(word_id, ent, freq);
          sum_unorm_p_tilda_w_e_local += freq;
          numWordsPerEntity += 1
        }
      }
      
      entToNumNonzeroWordProbs.put(ent, numWordsPerEntity)
      
      assert(sum_unorm_p_tilda_w_e_local <= total, " Entity e = " + ent + " sum_p_tilda_w_e = " + sum_unorm_p_tilda_w_e_local + " total = " + total)
      
      sum_unnorm_p_tilda_w_e.put(ent, sum_unorm_p_tilda_w_e_local);
      totalNumEnts += 1
    }

    System.err.println("Done loading word freq index p(w|e). Num ents = " +
        totalNumEnts); // + " size = " + entToWordFreq.size);
  }
}
