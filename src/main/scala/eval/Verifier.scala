package eval

import scala.collection.mutable.ArrayBuffer
import gnu.trove.set.hash.THashSet
import scala.collection.JavaConversions._

/**
 * From a set on annotated token spans and ground truth annotations computes 
 * precision, recall and lists of correct, wrong and unfound annotations. 
 */
class Verifier[T] {
  private var precision = 0.0;
  private var recall = 0.0;
  
  val correctAnnotations = new ArrayBuffer[T];
  val wrongAnnotations = new ArrayBuffer[T];
  val notFoundAnnotations = new ArrayBuffer[T];
  
  private var percMissingTkspsFromIndex = 0.0;
  private var percMissingGroundCandidateFromIndex = 0.0;

  /////////////////////////////////////////////////////////////////////////////
  
  def computeResults(solution : THashSet[T], groundTruth : THashSet[T]) = {
    var good = 0;
    for (annotation <- solution) {
      if (groundTruth.contains(annotation)) {
        good += 1
        correctAnnotations.add(annotation);
      } else {
        wrongAnnotations.add(annotation);
      }
    }
    
    for (annotation <- groundTruth) {
      if (!solution.contains(annotation)) {
        notFoundAnnotations.add(annotation);
      }
    }
    
    precision = if (solution.size() > 0) good.toDouble / solution.size() else 1.0;
    recall = if (groundTruth.size() > 0) good.toDouble / groundTruth.size() else 1.0;
  }
  
  
  def solutionSize() : Int = {
    return correctAnnotations.size() + wrongAnnotations.size();
  }
  
  def groundTruthSize() : Int = {
    return correctAnnotations.size() + notFoundAnnotations.size();
  }
  
  def intersectionSize() : Int = {
    return correctAnnotations.size();
  }
  
  def getPrecision() : Double = {
    return precision;
  }
  
  def getRecall() : Double = {
    return recall;
  }

  def getCorrectAnnotations() : ArrayBuffer[T] = {
    return correctAnnotations;
  }

  def getWrongAnnotations() : ArrayBuffer[T] = {
    return wrongAnnotations;
  }  
  
  def getNotFoundAnnotations() : ArrayBuffer[T] = {
    return notFoundAnnotations;
  }

  override def toString() : String = {
    return "Precision: " + precision + "Recall:" + recall;
  }
  
  def setPercMissingTkspsFromIndex(x : Double) = {
    percMissingTkspsFromIndex = x;
  }
  
  def getPercMissingTkspsFromIndex() : Double = {
    return percMissingTkspsFromIndex;
  }

  def setPercMissingGroundCandidateFromIndex(x : Double) = {
    percMissingGroundCandidateFromIndex = x;
  }
  
  def getPercMissingGroundCandidateFromIndex() : Double = {
    return percMissingGroundCandidateFromIndex;
  }
 
  
  // Intersects this verifier with another one (computes the number of common solutions).
  def intersect(other : Verifier[T]) : Double = {
    var common = 0.0;
    val hashset = new THashSet[T]();
    for (t <- other.correctAnnotations) {
      hashset.add(t);
    }
    for (t <- other.wrongAnnotations) {
      hashset.add(t);
    }
    for (t <- correctAnnotations) {
      if (hashset.contains(t)) {
        common += 1
      }
    }
    for (t <- wrongAnnotations) {
      if (hashset.contains(t)) {
        common += 1;
      }
    }
    if (common == 0) return 0;
    return common / (wrongAnnotations.size() + correctAnnotations.size()) ;
  }
}
 