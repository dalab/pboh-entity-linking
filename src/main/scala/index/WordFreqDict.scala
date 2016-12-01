package index;

import gnu.trove.map.hash.THashMap
import gnu.trove.map.hash.TIntIntHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import gnu.trove.map.hash.TIntObjectHashMap

class WordFreqDict extends java.io.Serializable  {
  
  val INITIAL_SIZE = 600000;
  
  val wordStringToID = new TObjectIntHashMap[String](INITIAL_SIZE * 4 / 3 + 1);
  
  val wordIDtoString = new TIntObjectHashMap[String](INITIAL_SIZE * 4 / 3 + 1);

  val wordToFreq = new TIntIntHashMap(INITIAL_SIZE * 4 / 3 + 1);
  
  var vocabularySize = 0;
  var totalSumFreqs : Long = 0L;  

  def load(path : String) = {
    System.err.println("Loading word freq index p(w) from " + path + "...");

    val lines = scala.io.Source.fromFile(path).getLines

	lines.foreach(line => {
	  val tokens = line.split("\t");
	  val id = Integer.parseInt(tokens(0));
	  val word = tokens(1);
	  val freq = Integer.parseInt(tokens(2));
	  
	  wordStringToID.put(word, id);
	  wordToFreq.put(id, freq);
	  wordIDtoString.put(id, word);
	  
	  vocabularySize += 1;
	  totalSumFreqs += freq; 
	})

	assert(totalSumFreqs > 0);
    assert(vocabularySize == wordIDtoString.size());
    assert(vocabularySize == wordStringToID.size());
    assert(vocabularySize == wordToFreq.size());

    System.err.println("Done loading word freq index p(w). Size = " + wordToFreq.size());
  }
}
