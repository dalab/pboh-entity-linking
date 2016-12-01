package context

import index.WordFreqDict
import scala.collection.mutable.MutableList
import org.apache.commons.lang3.StringUtils
import java.util.regex.Pattern
import java.util.ArrayList
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer


object TextSplittingInWords {

  // Does porter stemmer + stop words removal.
  def splitDocInWords(doc : String, sw : StopWords) : List[String] = {
    val rez = new MutableList[String]
    
    val norm_doc = doc
    	.replace(133.toChar, '.')
        .replace(151.toChar, '-')
        .replace(150.toChar, '-')
        .replace(145.toChar, '\'')
        .replace(146.toChar, '\'')
        .replace(147.toChar, '"')
        .replace(148.toChar, '"')
        
    val tokens = StringUtils.split(norm_doc, " \t\n\r\f—/*`\"'()[]{},‘’“”;?!:_#^*+=<>")
    
    // Just words with letters: val p = Pattern.compile("[^a-zA-Z0-9\\-\\.]")
    for (w <- tokens) {
      var ww = w.toLowerCase()
      while (ww.length() > 0 && (ww.endsWith(".") || ww.endsWith("-"))) { // U.S. ==> U.S , but that's ok.
        ww = ww.substring(0, ww.length() - 1)
      }
      while (ww.length() > 0 && (ww.startsWith(".") || ww.startsWith("-"))) {
        ww = ww.substring(1)
      }

      ww = PorterStemmer.stem(ww)
      if (ww != "" && !sw.isStopWord(ww)) { //&& !p.matcher(ww).find()) {
        rez += ww
      }
    }
    rez.toList
  }  
  
  /*
   * One should only use this, not the one above!
   */
  def stringToListOfWordIDs(
      text : String, 
      sw : StopWords,
      wordFreqDict : WordFreqDict) : List[Int] = {
    
    splitDocInWords(text, sw)
    	.filter(w => wordFreqDict.wordStringToID.containsKey(w))
    	.map(w => wordFreqDict.wordStringToID.get(w).toInt)
  }  
  
  
  
  def getContextWords(startMention : Int, mention : String, textWords : List[Int], sw : StopWords, wordFreqDict : WordFreqDict) : Array[Int] = {
    val mentionWordIds = TextSplittingInWords.stringToListOfWordIDs(mention, sw, wordFreqDict)
    var res = new ArrayBuffer[Int]
  
    if (mentionWordIds.length > 0) {
    breakable {
      for (i <- startMention to -1 by -1) {
        assert (i >= 0, "ERROR: did not find mention m = " + mention + "; at index = " + startMention +
            "; wordAtOffset = " + wordFreqDict.wordIDtoString.get(textWords(startMention)))
        if (textWords(i) == mentionWordIds(0)) {
          var isMatch = true
          breakable {
            for (j <- 0 until mentionWordIds.length) {
              if (mentionWordIds(j) != textWords(i+j)) {
                isMatch = false
                break
              }
            }
          }
          if (isMatch) {
            val K = EntityWordsProbs.K
            for (j <- i-1 to Math.max(0, i - K) by -1) {
              res += textWords(j)
            }
            for (j <- (i + mentionWordIds.size) until Math.min(i + mentionWordIds.size + K - 1, textWords.size)) {
              res += textWords(j)
            }            
            break
          }
        }
      }
    }
    } else { // Some weird examples as "Such" which is removed from the dictionary
      val K = EntityWordsProbs.K
      for (j <- startMention-1 to Math.max(0, startMention - K) by -1) {
        res += textWords(j)
      }
      for (j <- startMention until Math.min(startMention + K - 1, textWords.size)) {
        res += textWords(j)
      }            
    }
    
    res.toArray
  }  
  
      
  def getContextWords(mention : String, textWords : List[Int], sw : StopWords, wordFreqDict : WordFreqDict) : Array[Int] = {
    val mentionWordIds = TextSplittingInWords.stringToListOfWordIDs(mention, sw, wordFreqDict)
    var res = new ArrayBuffer[Int]
  
    if (mentionWordIds.length > 0) {
      breakable {
      for (i <- 0 until textWords.length) {
        if (textWords(i) == mentionWordIds(0)) {
          var isMatch = true
          breakable {
            for (j <- 0 until mentionWordIds.length) {
              if (mentionWordIds(j) != textWords(i+j)) {
                isMatch = false
                break
              }
            }
          }
          if (isMatch) {
            val K = EntityWordsProbs.K
            for (j <- i-1 to Math.max(0, i - K) by -1) {
              res += textWords(j)
            }
            for (j <- (i + mentionWordIds.size) until Math.min(i + mentionWordIds.size + K - 1, textWords.size)) {
              res += textWords(j)
            }            
            break
          }
        }
      }
      }
    }
    res.toArray
  }   
}