package eval

import md.Mention
import utils.Normalizer
import com.google.common.collect.Iterables
import index.AllIndexesBox
import gnu.trove.set.hash.THashSet
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import index.EntIDToNameIndex
import loopybeliefpropagation.ScorerFull
import index.MentEntsFreqIndexWrapper

abstract class VerifyEDAbstract(allIndexesBox : AllIndexesBox) {
    
  // To be init
  val mentEntsCache : MentEntsFreqIndexWrapper

  var totalGroundTruthAnnots : Long = 0
  
  var missingTokenSpansInIndex = 0
  
  var missingCandForGroundEntInIndex = 0
  
  var totalGroundTruthAnnotsLocal : Long = 0
  
  private var missingTokenSpansInIndexLocal = 0
  private var missingCandForGroundEntInIndexLocal = 0
  
  private var missingMentionStrings = new THashSet[String]
  private var missingGroundTruthInMentionCandidateList = new THashSet[String]

  // Creates a list of mentions from a list of strings.
  // Also sets a candidate list for each mention.
  def getMentions(
      annotations : THashSet[Annotation],
      mentEntsCache : MentEntsFreqIndexWrapper) : Array[Mention] = {
    
    missingTokenSpansInIndexLocal = 0;
    missingCandForGroundEntInIndexLocal = 0;
    totalGroundTruthAnnotsLocal = annotations.size();
    
    missingMentionStrings = new THashSet[String]
    missingGroundTruthInMentionCandidateList = new THashSet[String]
    
    println(" =============== Start Entity Linking pipeline ============ ")
    val groundTruthMentions = new ListBuffer[Mention]
    
    for (annotation <- annotations) {
      val offset = annotation.getMention.getOffset
      val length = annotation.getMention.getLength
      val tokenSpan = annotation.getMention.getNgram
      val normalizedTokenSpan = tokenSpan
      val groundTruthEnt = annotation.getGroundTruthEntity
      
      // If mention is not in our index, then we do not use it for EL, but we will 
      // consider it for our final accuracy.
      if (!mentEntsCache.containsMention(normalizedTokenSpan)) {
        missingTokenSpansInIndexLocal += 1
        missingMentionStrings.add("MISSING TOKEN SPAN IN INDEX : original m = " +  tokenSpan +
            "; normalized m = " +  normalizedTokenSpan +
            " entity = " + allIndexesBox.entIDToNameIndex.get(annotation.getEntity()) +
            " ent id = " + annotation.getEntity());
      } else {
        // If the candidate is not in the list of possible entities for this name, then we have
        // no chance to find it.
        if (mentEntsCache.getCandidateProbability(normalizedTokenSpan, annotation.getEntity()) == 0) {
          missingGroundTruthInMentionCandidateList.add("MISSING CAND IN INDEX for original m = " +  tokenSpan +
              "; normalized m = " +  normalizedTokenSpan +
              " entity = " + allIndexesBox.entIDToNameIndex.get(annotation.getEntity()) +
              " ent id = " + annotation.getEntity())
          missingCandForGroundEntInIndexLocal += 1
        }        

        val mention = new Mention(annotation.getMention.getOriginalNgram, offset, length);
        mention.setGroundTruthEntity(annotation.getEntity()); /// For debug in the scorer.
        groundTruthMentions.add(mention);
      }
    }
    
    missingTokenSpansInIndex += missingTokenSpansInIndexLocal;
    missingCandForGroundEntInIndex += missingCandForGroundEntInIndexLocal;
    totalGroundTruthAnnots += totalGroundTruthAnnotsLocal;
    
    return groundTruthMentions.toArray;
  }


  def verifyAnnotations(
      name : String,
      results : THashSet[Annotation],
      groundTruthAnnotations : THashSet[Annotation],
      scorer : ScorerFull,
      printOutputInfo : Boolean) : Verifier[Annotation] = {

    assert (totalGroundTruthAnnotsLocal == groundTruthAnnotations.size())

    val filePath = if (results.size() > 0) Iterables.get(results, 0).getFilePath() else ""
      
    val verifier = new Verifier[Annotation]()
    verifier.computeResults(results, groundTruthAnnotations)

	
    val percMissingTkspsFromIndex =	missingTokenSpansInIndexLocal.toDouble / totalGroundTruthAnnotsLocal
    val percMissingGroundCandidateFromIndex = missingCandForGroundEntInIndexLocal.toDouble / totalGroundTruthAnnotsLocal

    verifier.setPercMissingGroundCandidateFromIndex(percMissingGroundCandidateFromIndex)
    verifier.setPercMissingTkspsFromIndex(percMissingTkspsFromIndex);
	
    if (printOutputInfo) {
      println("\n ------------ Results for " + name + " ------------------------")
      println("\n File:" + filePath + " \nPrecision:" + verifier.getPrecision() + " Recall:" + verifier.getRecall());
      println() 
      for (s <- missingMentionStrings.iterator()) {
        println(s)
      }
   
      println() 
      for (s <- missingGroundTruthInMentionCandidateList.iterator()) {
        println(s)
      }
    
      println("\nGround truth size = " + totalGroundTruthAnnotsLocal)
      println("Percent missing tkspans from index = " + percMissingTkspsFromIndex)
      println("Percent missing real ent from candidates index = " + percMissingGroundCandidateFromIndex);
    
      /////// TO DO : delete this when running on big data.
      outputAnnotations("\nGood solutions:", verifier.getCorrectAnnotations.toArray, allIndexesBox.entIDToNameIndex, scorer, verifier)
      outputAnnotations("\nFalse positives (wrong assigned):", verifier.getWrongAnnotations.toArray, allIndexesBox.entIDToNameIndex, scorer, verifier)
      outputAnnotations("\nFalse negatives (not found):", verifier.getNotFoundAnnotations.toArray, allIndexesBox.entIDToNameIndex, scorer, verifier)
    }
	
    return verifier;
  }

  def outputAnnotations(
      banner : String,
      annotations : Array[Annotation], 
      entIDToNameIndex: EntIDToNameIndex,
      scorer : ScorerFull,
      verifier : Verifier[Annotation] ) = {
    
    println(banner)

    for (annotation <- annotations) {
      println("Mention normalized = " + annotation.getMention.getNgram + "; e = "
          + entIDToNameIndex.get(annotation.getEntity()) +
          "; Ground truth e =  " + entIDToNameIndex.get(annotation.getGroundTruthEntity()))
      }
    println
  }  
	
  def printOverallResults() = {
    val perAllFilesMaximumAchievableRecall = 1 - (missingTokenSpansInIndex.toDouble + missingCandForGroundEntInIndex) / totalGroundTruthAnnots;
    println("Maximum achievable recall per all files " + perAllFilesMaximumAchievableRecall)
    
    val percMissingTkspsFromIndex = missingTokenSpansInIndex.toDouble / totalGroundTruthAnnots
    val percMissingGroundCandidateFromIndex = missingCandForGroundEntInIndex.toDouble / totalGroundTruthAnnots

    println("Overall Ground truth size = " + totalGroundTruthAnnots);
    println("Overall percent missing tkspans from index = " + percMissingTkspsFromIndex);
    println("Overall percent missing real ent from candidates index = " + percMissingGroundCandidateFromIndex);
  }
}
