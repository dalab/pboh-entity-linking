package console_el

import eval.VerifyEDLBPForListOfWords
import java.util.ArrayList
import index.EntNameToIDIndex
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.commons.lang.StringUtils
import index.AllIndexesBox
import eval.cweb.RunLoopyAgainstAllGoogleClweb
import loopybeliefpropagation.ScorerWeights
import gnu.trove.set.hash.THashSet
import scala.collection.mutable.ArrayBuffer
import eval.Annotation
import index.MentEntsFreqIndexWrapper
import loopybeliefpropagation.RhoCache
import md.Mention
import context.StopWords
import context.TextSplittingInWords
import gnu.trove.map.hash.THashMap
import gnu.trove.map.hash.TIntObjectHashMap
import gnu.trove.set.hash.TIntHashSet


/*
 * Allows as input a set of mentions to be disambiguated and a set of entities 
 * that might co-occur in the same doc with the given mentions.
 * 
 * To run: 
 * MASTER=spark://dco-node040.dco.ethz.ch:7077 ~/spark/bin/spark-submit
 *  --class "EL_LBP_Spark"  --executor-memory 100G --total-executor-cores 128
 *  --jars lib/el_lbp_clueweb_list_words_2.10-1.0.jar lib/el-lbp.jar consoleLoopy
 *  
 *  or: scala -J-Xmx60g -J-Xms60g -classpath "lib/*" EL_LBP_Spark consoleEntityLinking /path/to/the/index/files/
 *  
 */
 */

// root = /path/to/the/index/files/ e.g. /mnt/cw12/Octavian/el-lbp-marina/marinah/wikipedia/
class ConsoleEntityLinking {
  
  val allIndexesBox = new AllIndexesBox("mentionEntsFreqIndex,entIDToNameIndex,entNameToIDIndex, redirectIndex, wikipEntityCooccurrIndex,wordFreqDict,wordEntityProbs")
   
  def extractInputVectors(line_mentions : String, line_entities : String) 
  	: (Array[String], Array[Int], Array[Int]) = {

     var tkspans = new ArrayBuffer[String]
     var entities = new ArrayBuffer[Int]
     var offsets = new ArrayBuffer[Int]

     if (line_mentions.trim().length() == 0) {
       return (tkspans.toArray, entities.toArray, offsets.toArray)
     }
     
     val terms_mentions = line_mentions.split(",")
     for (i <- 0 until terms_mentions.length) {
       if (terms_mentions(i).trim().length() > 0) {
         tkspans += terms_mentions(i).trim()
         offsets += i
       }
     }
     
     val mentEntsCache = new MentEntsFreqIndexWrapper(allIndexesBox, new RhoCache, tkspans.toArray, null, new ScorerWeights)
     for (i <- 0 until terms_mentions.length) {
       if (mentEntsCache.containsMention(tkspans(i))) {
         entities += mentEntsCache.getMostFrequentEntity(tkspans(i))
       } else {
         entities += -1
       }
     }
     
     if (line_entities != null && line_entities.size > 0) {
       val terms_entities = line_entities.split(",")
       var j = 0
       for (i <- 0 until terms_entities.length) {
         val entityString = StringUtils.capitalize(terms_entities(i).trim())
         val wikiCanonical = allIndexesBox.redirectIndex.getCanonicalURL(entityString)
         val entId = allIndexesBox.entNameToIDIndex.getTitleId(wikiCanonical)
         if (entId == allIndexesBox.entNameToIDIndex.NOT_CANONICAL_TITLE) {
           println("[WARNING] Entity \"" + wikiCanonical + "\" is not in our index enwiki-titles.txt and will be skipped.\n")
         } else {
           val offset = j + terms_mentions.length
           tkspans += wikiCanonical
           offsets += (1000000 + offset)
           entities += entId
           j += 1
         }
       }
     }
     (tkspans.toArray, entities.toArray, offsets.toArray)
  }   
  
  /*
   * Input:
   * line_mentions :  all the input mentions, separated by comma
   * line_entities : all entities co-occurring in the same doc with the above mentions, separated by comma; can be empty
   * 
   * Output: map<mention, (entity, score)> returned by the EL algorithm
   * 
   * Example: 
   * line_mentions: "michael jordan , program"
   * line_entities: "machine learning"
   * 
   * will give: 
   * michael jordan ----> Michael I. Jordan
   * program ----> Computer program
   * 
   * but if line_entities is an empty string, then you get:
   * michael jordan ----> Michael Jordan
   * program ----> Television program
   * 
   * @param loopy: true to run LBP, false to run ARGMAX 
   */
  def EntityLinkingAPI(
      line_mentions : String, 
      loopy : Boolean, 
      line_entities : String, 
      max_product : Boolean): Array[Annotation] = {
    
    val (tkspans, entities, offsets) = extractInputVectors(line_mentions, line_entities)
    val contexts = null
    val scorerWeights = new ScorerWeights
    val entityLinkingRunner = new VerifyEDLBPForListOfWords(allIndexesBox, tkspans, entities, contexts, offsets, scorerWeights)
   
    // Process and run the EL-LBP algorithm.
    val verifier = 
      if (loopy) {
        entityLinkingRunner.runEDLoopyOnOneInputList(max_product).verifier
      } else {
        entityLinkingRunner.runEDArgmaxOnOneInputList()
      }
   
    var ELAnnots = verifier.getCorrectAnnotations()
    for (anot <- verifier.getWrongAnnotations()) {
      ELAnnots += anot
    }
    ELAnnots.toArray
  }
  
  
  def consoleJustOutput(max_product : Boolean) = {
    var ok = true
    while (ok) {
      println("\nWrite mentions on a single line separated by commas:")
      val line_mentions = readLine()
      ok = line_mentions != null

      println("\nWrite entities that co-occur in the same doc on a single line " + 
          "separated by commas, " +
          "without the full URL (e.g. \"Barack Obama\"). Empty if none. ")
      val line_entities = readLine()

      println()
      
      if (ok) {
        val mapLoopy = EntityLinkingAPI(line_mentions, true, line_entities, max_product)        

        println("\n ========= RESULTS Loopy =========== ")
        for (a <- mapLoopy) {
          println(a.getMention.getNgram + " ----> " + allIndexesBox.entIDToNameIndex.get(a.getEntity) + " -- with score: " + a.getScore)  
        }

        val mapARGMAX = EntityLinkingAPI(line_mentions, false, line_entities, max_product)        

        println("\n ========= RESULTS ARGMAX =========== ")
        for (a <- mapARGMAX) {
          println(a.getMention.getNgram + " ----> " + allIndexesBox.entIDToNameIndex.get(a.getEntity) + " -- with score: " + a.getScore)          
        }        
      }
    }
  }
  
  def consoleCompareWithArgmax(max_product : Boolean) = {
    var ok = true
    while (ok) {
      println("\nWrite mentions on a single line separated by commas:")
      val line_mentions = readLine()
      ok = line_mentions != null

      println("\nWrite entities that co-occur in the same doc on a single line " + 
          "separated by commas (or just enter if no entities available), " +
          "without the full URL (e.g. \"Barack Obama\":")
      val line_entities = readLine()

      println()
      
      if (ok) {
        val (tkspans, entities, offsets) = extractInputVectors(line_mentions, line_entities)        
        val scorerWeights = new ScorerWeights
        val contexts = null
      
        val entityLinkingRunner = new VerifyEDLBPForListOfWords(allIndexesBox, tkspans, entities, contexts, offsets, scorerWeights)
               
        
        // Process and run the EL-LBP algorithm.
        val verifierLoopy =
          entityLinkingRunner.runEDLoopyOnOneInputList(max_product).verifier
          
        // Process and run ARGMAX version.
        val verifierARGMAX = 
          entityLinkingRunner.runEDArgmaxOnOneInputList()
      
        // Scorer for debug only.
        val scorer =
          entityLinkingRunner.getLoopyScorerForOneInputList(tkspans, entities, contexts, offsets, scorerWeights)   
    
        RunLoopyAgainstAllGoogleClweb.printDiffsLoopyArgmax(
            verifierLoopy, verifierARGMAX,
            tkspans, entities, offsets, allIndexesBox, scorer)            
      }
    }
  }
  
  
  ///////////// GERBIL evaluation ////////////////////////
  
  private def fillMentionsWithContext(
      docText : String, 
      mentions : Array[Mention], 
      allIndexesBox : AllIndexesBox) : Array[(Mention, Array[Int])] = {
    
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    
        
    val offsetToMentionMap = new TIntObjectHashMap[Mention]
    val endMentionsOffsets = new TIntHashSet // index of the first char after a mention

    for (m <- mentions) {
      offsetToMentionMap.put(m.getOffset, m)
      endMentionsOffsets.add(m.getOffset + m.getLength)
    }
    
    val insertedText = new java.lang.StringBuilder
    
    for (i <- 0 until docText.length()) {
      if (offsetToMentionMap.containsKey(i)) {
        if (i > 0 && docText.charAt(i -1) != ' ') {
          insertedText.append(' ')
        }
      }
      if (endMentionsOffsets.contains(i) && docText.charAt(i) != ' ') {
        insertedText.append(' ')        
      } 
      if (offsetToMentionMap.containsKey(i)) {
        insertedText.append("!@START#%" + i + "@$END*")        
      }
      insertedText.append(docText.charAt(i))      
    }

    
    var docWordsMentionsAndEntities = new ArrayBuffer[(String, Mention)] // List[(word, mention)]    
    val tokens = insertedText.toString().split(" ")
    for (word <- tokens) {
      if (word.contains("!@START#%") && word.contains("@$END*")) {
        val i = word.substring(word.indexOf("!@START#%") + "!@START#%".length(), word.indexOf("@$END*")).toInt
        val remainingWord = word.substring(word.indexOf("@$END*") + "@$END*".length())
        docWordsMentionsAndEntities += ((remainingWord, offsetToMentionMap.get(i)))
      } else {
        docWordsMentionsAndEntities += ((word, null))
      }
    }
    
    
    val result = new ArrayBuffer[(Mention, Array[Int])](mentions.length)         
    val sw = new StopWords
   
    val textWordsIds =
      TextSplittingInWords.stringToListOfWordIDs(docWordsMentionsAndEntities.map(_._1).mkString(" "), sw, allIndexesBox.wordFreqDict)
      
    var indexInTextWordsIds = 0
    for (x <- docWordsMentionsAndEntities) {
      val word = x._1
      val m = x._2
      if (m != null) {
        val mentionString = m.getOriginalNgram
        val context = TextSplittingInWords.getContextWords(indexInTextWordsIds, mentionString, textWordsIds, sw, allIndexesBox.wordFreqDict)
        result += ((m, context))
 
        if (context.length == 0) {
          println("$$$$$$$$$$$$$$$ 0 LENGTH CONTEXT ######################## m.ngram = " + m.getNgram + "; original = " + m.getOriginalNgram + 
              "; offset= " + docText.indexOf(m.getOriginalNgram));
          if (docText.indexOf(m.getOriginalNgram) >= 0) {
            println(" -----> substring = " + 
                docText.substring(Math.max(0, docText.indexOf(m.getOriginalNgram) - 20), 
                    Math.min(docText.length() - 1, docText.indexOf(m.getOriginalNgram) + m.getOriginalNgram.length() + 20)))
          }
        }
      }
      indexInTextWordsIds += TextSplittingInWords.stringToListOfWordIDs(word, sw, allIndexesBox.wordFreqDict).size
    }
    result.toArray
  }   

  
  def evalOneDocWithContext(
      docText : String, 
      mentions : Array[Mention],
      max_product : Boolean) : Array[Annotation] = {
    
    val mentionsAndContext = fillMentionsWithContext(docText, mentions, allIndexesBox)
    
    val weights = new ScorerWeights
    weights.g = 0.5
    weights.b = 0.075
    weights.delta_w_e = 1.0
    weights.xi = 0.5
    weights.delta_cocit = 0.5
    weights.cocit_e_e_param = 0.01    
    
    var tkspans = new ArrayBuffer[String]
    var entities = new ArrayBuffer[Int]
    var contexts = new THashMap[String, Array[Int]]
    var offsets = new ArrayBuffer[Int]

    // Prepare vector of ground truth mentions and entities.
    for (i <- 0 until mentionsAndContext.size) {
      val mention = mentionsAndContext(i)._1.getOriginalNgram
      val ent = 100
      val context = mentionsAndContext(i)._2
      val offset = mentionsAndContext(i)._1.getOffset
      
      // Keep all mentions (including duplicates).
      tkspans += mention
      offsets += offset
      entities += ent
      contexts.put(mention, context)

      println (" ==== c === :: m = " + mention + " ; c = " + context.map(c => allIndexesBox.wordFreqDict.wordIDtoString.get(c)).mkString(" "))

    }

    if (tkspans.size < 1000)  { //HACK to make it take less time
      // Run EL-LBP and ARGMAX:
      val entityLinkingRunner = 
        new VerifyEDLBPForListOfWords(allIndexesBox, tkspans.toArray,entities.toArray, contexts, offsets.toArray, weights)
      
      if (tkspans.size > 0) {
        // Process and run the EL-LBP algorithm.
        var lbpresults = entityLinkingRunner.runEDLoopyOnOneInputList(max_product) //.runEDArgmaxOnOneInputList
        
        val verifier = lbpresults.verifier
        
        var ELAnnots = verifier.getCorrectAnnotations()
        for (anot <- verifier.getWrongAnnotations()) {
          ELAnnots += anot
        }
        return ELAnnots.toArray
      }
    }

    new Array[Annotation](0)    
  }  
  
}