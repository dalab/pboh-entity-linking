package loopybeliefpropagation;

import eval.Annotation
import index.EntIDToNameIndex
import md.Mention
import gnu.trove.map.hash.THashMap
import gnu.trove.map.hash.TObjectDoubleHashMap
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import gnu.trove.set.hash.THashSet
import index.AllIndexesBox
import index.MentEntsFreqIndexWrapper


/**
 * Loopy Belief Propagation Algorithm. 
 * The specific message update equations are given by a class that implements Scorer.
 * 
 * @param mentions = Mentions detected by mention detection phase.
 * @param scorer = Implements message update rules. Computes the message update equations and the final scores.
 * @param iterations = Maximum number of iterations. Needed because it is possible that the algorithm never converges.
 */
class LoopyBeliefPropagation(
    mentions : Array[Mention], 
    iterations : Int, 
    scorer : Scorer, 
    allIndexesBox : AllIndexesBox,
    mentEntsCache : MentEntsFreqIndexWrapper) {
  
  type ScoresMapType = TObjectDoubleHashMap[(Mention, Int)]
  
  // Threshold for checking convergence.
  val EPS = 0.00001;
  
  // Maps each mention to the best entity ID
  private var solution = new THashMap[Mention, EntityScorePair]

	
  /**
   * LBP with max-product or sum-product.
   * 
   * Runs the mention detection algorithm on the input mentions and applies until convergence the
   * message update equations specified by Scorer. The final results are kept in the mentions 
   * variable and can be retrieved by getSolutionAnnotations() as Annotation objects, or  by
   * getSolutionNameAnnotations() as NameAnnotation objects. 
   * 
   * For sum-product:
   * 	http://www.yaroslavvb.com/papers/cohn-scaling.pdf
   * Messages in log space.
   * Normalize all messages from Y_j to Y_i to sum to 1
   * Trick: log(exp(x) + exp(y)) = x + log(1 + exp(y-x)), if y <= x
   * 
   * @param max_product: true if running max product algorithm, false if running sum-product.
   */
  def solve(max_product : Boolean) : LBPTimeConvergence = {
    solution = new THashMap[Mention, EntityScorePair]
    if (max_product) {
      return solveMaxProd();
    }
    return solveSumProd();
  }

  def solveMaxProd() : LBPTimeConvergence = {
    val start = System.currentTimeMillis()
    
    var totalNumEntsInGraph = 0
    for (m <- mentions) {
      totalNumEntsInGraph += mentEntsCache.getCandidateEntities(m.getNgram).length
    }
    
    
    var oldMessages = initializeMessages() // all set to 0
    var newMessages : MessagesMap = null
    
    // Stores the LBP scores for each candidate entity and each mention at the current iteration.
    var scores : ScoresMapType = null
    var prevScores : ScoresMapType = null
    
    var delta = 0.0
    var iteration = 0
    breakable {
      while (iteration < iterations) {
        iteration += 1
        
        newMessages = new MessagesMap()
        for (from <- mentions) {
          for (to <- mentions) {
            if (!from.equals(to)) {
              var uninitialized = true
              var normalizingConstant = 0.0
              
              for (entityTo <- mentEntsCache.getCandidateEntities(to.getNgram)) {
                // Computes the value of one message as the best value over all entityFrom.
                var bestScore = 0.0;
                var bestEntity = -1;
                for (entityFrom <- mentEntsCache.getCandidateEntities(from.getNgram)) {
                  val score = scorer.computeMessageScore(from, to, entityFrom, entityTo, oldMessages)
                  if (bestEntity == -1 || score > bestScore) {
                    bestEntity = entityFrom;
                    bestScore = score;
                  }
                }
                
                // normalizingConstant = min(bestScore)
                if (uninitialized || normalizingConstant > bestScore) {
                  uninitialized = false;
                  normalizingConstant = bestScore;
                }
                
                // Add a message to the current message set.
                newMessages.putMsg(new Message(from, to, entityTo), bestScore);
              }
              
              // Normalize messages s.t. the lowest one is zero.
              for (entityTo <- mentEntsCache.getCandidateEntities(to.getNgram)) {
                val message = new Message(from, to, entityTo)
                val normalizedScore = newMessages.get(message) - normalizingConstant
                newMessages.putMsg(message, normalizedScore)
              }
            }
          }
        }
        
        // Compute scores for each possible disambiguation.
        scores = scorer.computeScores(newMessages)
        if (iteration != 1) {
          // Computes the maximum difference between the previous scores and the current scores.
          delta = computeDelta(scores, prevScores);
          // If delta is small enough, LBP converged.
          if (delta < EPS) {
            break;
          }
        }
        
        oldMessages = newMessages;
        prevScores = scores;
      }
    }
    
    val end = System.currentTimeMillis();
    
    // Find avg num of candidates per mention
    var avgNumCandidates = 0.0
    for (m <- mentions) {
      avgNumCandidates += mentEntsCache.getCandidateEntitiesCount(m.getNgram)
    }
    avgNumCandidates /= Math.max(1, mentions.length)

    //// Done finding avg num candidates per mention
    
    println("\nConverged in " + iteration + " iterations for mentions size:" + mentions.size + " num entities in graph = " + totalNumEntsInGraph);
    var converged = true
    if (delta > EPS) {
      converged = false;
      System.out.println("******** LBP has NOT converged for mentions size:" +  mentions.size + ". Finishing delta:" + delta);
	}
    val lbpstats = new LBPTimeConvergence((end - start), iteration ,converged , avgNumCandidates);
    
    solution = scorer.computeSolution(newMessages, true);
    
    System.out.println("LBP Max-product spent time: " + (end - start) + " for mentions size:" + mentions.size);
    return lbpstats;
  }
  

  def solveSumProd() : LBPTimeConvergence = {
    val start = System.currentTimeMillis()
    
    var oldMessages = initializeMessages() // all set to 0
    var newMessages : MessagesMap = null
    
    // Stores the LBP scores for each candidate entity and each mention at the current iteration.
    var scores : ScoresMapType = null
    var prevScores : ScoresMapType = null
    
    var delta = 0.0
    var iteration = 0
    breakable {
      while (iteration < iterations) {
        iteration += 1
        
        newMessages = new MessagesMap()
        for (from <- mentions) {
          for (to <- mentions) {
            if (!from.equals(to)) {
              var logSumExpMessages = -10000.0; // for message normalization
              
              for (entityTo <- mentEntsCache.getCandidateEntities(to.getNgram)) {
                var msgVal = -10000.0; // The value of the message(from,to,entityTo).
                
                // Computes the value of one message as the log of sum of exp of all other messages.
                for (entityFrom <- mentEntsCache.getCandidateEntities(from.getNgram)) {
                  val score = scorer.computeMessageScore(from, to, entityFrom, entityTo, oldMessages);
                  
                  // Use the trick: log(exp(x) + exp(y)) = x + log(1 + exp(y-x)), if y <= x
                  if (msgVal == -10000.0) {
                    msgVal = score;
                  } else if (score <= msgVal) {
                    val valueToAdd = Math.log(1 + Math.exp(score - msgVal));
                    msgVal = msgVal + valueToAdd;
                  } else {
                    val valueToAdd = Math.log(1 + Math.exp(msgVal - score));
                    msgVal = score + valueToAdd;
                  }
                }
 
                // Add a message to the current message set.
                newMessages.putMsg(new Message(from, to, entityTo), msgVal);
                
                if (logSumExpMessages == -10000.0) {
                  logSumExpMessages = msgVal;
                } else if (logSumExpMessages <= msgVal) {
                  val valueToAdd = Math.log(1 + Math.exp(logSumExpMessages - msgVal));
                  logSumExpMessages = msgVal + valueToAdd;
                } else {
                  val valueToAdd = Math.log(1 + Math.exp(msgVal - logSumExpMessages));
                  logSumExpMessages = logSumExpMessages + valueToAdd;
                }
              }
              
              
              // Normalize the log-messages s.t. the sum of their exponentials is 1.
              for (entityTo <- mentEntsCache.getCandidateEntities(to.getNgram)) {
                val message = new Message(from, to, entityTo);
                val normalizedScore = newMessages.get(message) -  logSumExpMessages;
                newMessages.putMsg(message, normalizedScore);
              }
            }
          }
        }
        
        // Compute scores for each possible disambiguation.
        scores = scorer.computeScores(newMessages)
        if (iteration != 1) {
          // Computes the maximum difference between the previous scores and the current scores.
          delta = computeDelta(scores, prevScores);
          // If delta is small enough, LBP converged.
          if (delta < EPS) {
            break;
          }
        }
        
        oldMessages = newMessages;
        prevScores = scores;
      }
    }
    
    val end = System.currentTimeMillis();
    
    // Find avg num of candidates per mention
    var avgNumCandidates = 0.0
    for (m <- mentions) {
      avgNumCandidates += mentEntsCache.getCandidateEntitiesCount(m.getNgram)
    }
    avgNumCandidates /= mentions.length
    //// Done finding avg num candidates per mention
    
    println("Converged in " + iteration + " iterations for mentions size:" + mentions.size);
    var converged = true
    if (delta > EPS) {
      converged = false;
      System.out.println("******** LBP has NOT converged for mentions size:" +  mentions.size + ". Finishing delta:" + delta);
	}
    val lbpstats = new LBPTimeConvergence((end - start), iteration ,converged , avgNumCandidates);
    
    solution = scorer.computeSolution(newMessages, true);
    
    System.out.println("LBP Sum-product spent time: " + (end - start) + " for mentions size:" + mentions.size);
    return lbpstats;
  }  
  

  /**
   * @param scores1 maps candidate to LBP score
   * @param scores2 maps candidate to LBP score
   * @return maximum delta score of a candidate in the two maps
   */
  private def computeDelta(scores1 : ScoresMapType, scores2 : ScoresMapType) : Double = {
    var maxDelta = 0.0;
    for (candidate <- scores1.keySet()) {
      val score1 = scores1.get(candidate);
      val score2 = scores2.get(candidate);
      maxDelta = Math.max(maxDelta, Math.abs(score1 - score2));
    }
    return maxDelta
  }

  /**
   * Initializes messages to 0.
   */
  private def initializeMessages() : MessagesMap = {
    val result = new MessagesMap();
    for (from <- mentions) {
      for (to <- mentions) {
        if (!from.equals(to)) {
          for (entity <- mentEntsCache.getCandidateEntities(to.getNgram)) {
            val message = new Message(from, to, entity);
            result.putMsg(message, 0.0);
          }
        }
      }
	}
    return result;
  }	


  /**
   * Converts the solution into a set of Annotation objects.
   * If the solve method has not been run, it returns an empty set.
   * @param filepath	The filename stored by the Annotation objects.
   * @return A set of Annotation computed by LBP
   */
  def getSolutionAnnotations(filePath : String) : THashSet[Annotation] = {
    val annotations = new THashSet[Annotation]()
    for (entry <- solution.entrySet()) {
      val mention = entry.getKey();
      val entity = entry.getValue().entity;
      val score = entry.getValue().score;
      annotations.add(
          new Annotation(
              entity,
              score,
              mention,
              filePath,
              mention.getGroundTruthEntity))
	}
    return annotations;
  }

}
