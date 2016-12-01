package eval

import loopybeliefpropagation.LBPTimeConvergence
import gnu.trove.map.hash.THashMap

// Statistics for docs with the same number of mentions
class StatsForSameSizedDocs extends java.io.Serializable {
    var macroSumPrecisionLoopy :Double = 0.0
    var macroSumRecallLoopy :Double = 0.0
    
    var macroSumPrecisionARGMAX :Double = 0.0
    var macroSumRecallARGMAX :Double = 0.0
    
    
    var microSumSolutionLoopy : Double  = 0.0
    var microSumGroundTruthLoopy : Double  = 0.0
    var microSumIntersectionLoopy : Double  = 0.0
    
    var microSumSolutionARGMAX : Double  = 0.0
    var microSumGroundTruthARGMAX : Double  = 0.0
    var microSumIntersectionARGMAX : Double  = 0.0
    
    
    var macroCommonLoopyArgmax : Double = 0.0
    var microCommonLoopyArgmax : Double = 0.0
    
    var sumPercMissingGroundCandidateFromIndex : Double = 0.0
    var sumPercMissingTkspsFromIndex : Double = 0.0
    
    // Running time of LBP
    var runTimeLBP : Long = 0
    // Convergence number of iterations in LBP
    var convergenceItersLBP : Int = 0
    // In how many docs did the LBP converge.
    var converged : Int = 0
    
    var avgNumCandPerMention : Double = 0.0
    
    var numVisitedDocs = 0
    var numSolvedMentions = 0
    var numGroundTruthMentions = 0

    
    def addOneDoc(
        verifierLoopy : Verifier[Annotation], 
        verifierARGMAX : Verifier[Annotation], 
        lbpstats : LBPTimeConvergence) {
      assert(verifierLoopy.groundTruthSize() == verifierARGMAX.groundTruthSize())
      
      macroSumPrecisionLoopy += verifierLoopy.getPrecision()
      macroSumRecallLoopy += verifierLoopy.getRecall()
      
      macroSumPrecisionARGMAX += verifierARGMAX.getPrecision()
      macroSumRecallARGMAX += verifierARGMAX.getRecall()
      
      microSumSolutionLoopy += verifierLoopy.solutionSize()
      microSumGroundTruthLoopy += verifierLoopy.groundTruthSize()
      microSumIntersectionLoopy += verifierLoopy.intersectionSize()
        
      microSumSolutionARGMAX += verifierARGMAX.solutionSize()
      microSumGroundTruthARGMAX += verifierARGMAX.groundTruthSize()
      microSumIntersectionARGMAX += verifierARGMAX.intersectionSize()

      
      macroCommonLoopyArgmax += verifierLoopy.intersect(verifierARGMAX)
      microCommonLoopyArgmax += verifierLoopy.solutionSize() * verifierLoopy.intersect(verifierARGMAX)
      
      sumPercMissingGroundCandidateFromIndex +=
        verifierLoopy.groundTruthSize() * verifierLoopy.getPercMissingGroundCandidateFromIndex()
      sumPercMissingTkspsFromIndex += 
        verifierLoopy.groundTruthSize() * verifierLoopy.getPercMissingTkspsFromIndex();  

      runTimeLBP  += lbpstats.time
      convergenceItersLBP += lbpstats.numiters
      converged += (if (lbpstats.converged) 1 else 0)
      avgNumCandPerMention += lbpstats.avgNumCandPerMention 
        
      numVisitedDocs += 1
      numSolvedMentions += verifierLoopy.solutionSize()
      numGroundTruthMentions += verifierLoopy.groundTruthSize()
    }
    
    def add(other : StatsForSameSizedDocs) {
      macroSumPrecisionLoopy += other.macroSumPrecisionLoopy 
      macroSumRecallLoopy += other.macroSumRecallLoopy 
      
      macroSumPrecisionARGMAX += other.macroSumPrecisionARGMAX 
      macroSumRecallARGMAX += other.macroSumRecallARGMAX 
      
      microSumSolutionLoopy += other.microSumSolutionLoopy 
      microSumGroundTruthLoopy += other.microSumGroundTruthLoopy 
      microSumIntersectionLoopy += other.microSumIntersectionLoopy 
        
      microSumSolutionARGMAX += other.microSumSolutionARGMAX 
      microSumGroundTruthARGMAX += other.microSumGroundTruthARGMAX 
      microSumIntersectionARGMAX += other.microSumIntersectionARGMAX 
      
      macroCommonLoopyArgmax += other.macroCommonLoopyArgmax
      microCommonLoopyArgmax += other.microCommonLoopyArgmax
      sumPercMissingGroundCandidateFromIndex += other.sumPercMissingGroundCandidateFromIndex
      sumPercMissingTkspsFromIndex += other.sumPercMissingTkspsFromIndex
      
      runTimeLBP  += other.runTimeLBP
      convergenceItersLBP += other.convergenceItersLBP
      converged += other.converged  
      avgNumCandPerMention += other.avgNumCandPerMention
      
      numVisitedDocs += other.numVisitedDocs
      numSolvedMentions  += other.numSolvedMentions 
      numGroundTruthMentions += other.numGroundTruthMentions
    }
}

class GlobalStats extends java.io.Serializable {
  val stats = new THashMap[String, StatsForSameSizedDocs]
  val keyList = List("GLOBAL", "1", "2", "3-10", "10-20", "20-40", "40-100", "100-200", "200-400", "400-1000")
  
  for (key <- keyList) {
    stats.put(key , new StatsForSameSizedDocs)
  }

  private def key(size : Int) : String = {
    assert(size > 0)
    if (size == 1) return "1"
    if (size == 2) return "2"
    if (size < 10) return "3-10"
    if (size < 20) return "10-20"
    if (size < 40) return "20-40"
    if (size < 100) return "40-100"
    if (size < 200) return "100-200"
    if (size < 400) return "200-400"
    if (size >= 400) return "400-1000"
    return "0"
  } 
  
  def addOneDoc(
      verifierLoopy : Verifier[Annotation], 
      verifierARGMAX : Verifier[Annotation], 
      lbpstats : LBPTimeConvergence) {
    stats.get("GLOBAL").addOneDoc(verifierLoopy, verifierARGMAX, lbpstats)
    stats.get(key(verifierLoopy.groundTruthSize())).addOneDoc(verifierLoopy, verifierARGMAX, lbpstats)
  }
  def add(other : GlobalStats) : GlobalStats = {
    for (key <- keyList) {
      stats.get(key).add(other.stats.get(key))
    }
    this
  }
  
  private def printOneKey(key : String) = {
      val localStats = stats.get(key)
      println("\nLooking at docs with " + key + " mentions:")
      println(key + " mentions " + " : num docs evaluated = " +
          localStats.numVisitedDocs + "; num mentions in solution = " + localStats.microSumSolutionLoopy + 
          " num mentions in ground truth = " + localStats.microSumGroundTruthLoopy )
          

      println("\n#################################")
      val precLoopy = localStats.microSumIntersectionLoopy / localStats.microSumSolutionLoopy 
      val recallLoopy = localStats.microSumIntersectionLoopy / localStats.microSumGroundTruthLoopy 
      println(key + " mentions " + " : micro F1 (per mention) Loopy : " +
          (100.0 * 2 * precLoopy * recallLoopy / (precLoopy + recallLoopy)))          
      println(key + " mentions " + " : micro accuracy/recall (per mention) Loopy : " +
          (100.0  * recallLoopy))          
      
      val macroPrecLoopy = localStats.macroSumPrecisionLoopy / localStats.numVisitedDocs
      val macroRecallLoopy = localStats.macroSumRecallLoopy / localStats.numVisitedDocs  
      println(key + " mentions " + " : MACRO F1 (per doc) Loopy : " +
          (100.0 * 2 * macroPrecLoopy * macroRecallLoopy / (macroPrecLoopy + macroRecallLoopy)))
      println(key + " mentions " + " : MACRO accuracy/recall (per doc) Loopy : " +
          (100.0 * macroRecallLoopy))
      println("###################################\n")

      val precARGMAX = localStats.microSumIntersectionARGMAX / localStats.microSumSolutionARGMAX
      val recallARGMAX = localStats.microSumIntersectionARGMAX / localStats.microSumGroundTruthARGMAX
      println(key + " mentions " + " : micro F1 (per mention) ARGMAX : " +
          (100.0 * 2 * precARGMAX * recallARGMAX / (precARGMAX + recallARGMAX)))     
      println(key + " mentions " + " : micro acc/recall (per mention) ARGMAX : " +
          (100.0 * recallARGMAX))   
          
      val macroPrecARGMAX = localStats.macroSumPrecisionARGMAX / localStats.numVisitedDocs
      val macroRecallARGMAX = localStats.macroSumRecallARGMAX / localStats.numVisitedDocs 
      println(key + " mentions " + " : MACRO F1 (per doc) ARGMAX : " +
          (100.0 * 2 * macroPrecARGMAX * macroRecallARGMAX / (macroPrecARGMAX + macroRecallARGMAX)))
      println(key + " mentions " + " : MACRO acc/recall (per doc) ARGMAX : " +
          (100.0 * macroRecallARGMAX ))
          
      println
      println(key + " mentions " + " : MACRO (per doc) common Loopy - ARGMAX : " +
          (100.0 * localStats.macroCommonLoopyArgmax / localStats.numVisitedDocs))
      println(key + " mentions " + " : micro (per mention) common Loopy - ARGMAX : " +
          (100.0 * localStats.microCommonLoopyArgmax / localStats.numSolvedMentions))

      println(key + " mentions " + " : micro (per mention) perc missing mentions from index : " +
        (100.0 * localStats.sumPercMissingTkspsFromIndex / localStats.numGroundTruthMentions))    
      println(key + " mentions " + " : micro (per mention) perc missing entities from mention index : " +
        (100.0 * localStats.sumPercMissingGroundCandidateFromIndex / localStats.numGroundTruthMentions))   
        
        
      // LBP running and convergence stats.
      println("\n***************")
      println(key + " mentions " + " : avg LBP running time (milliseconds) : " +
          ((0.0 + localStats.runTimeLBP) / localStats.numVisitedDocs))
      println(key + " mentions " + " : avg num iters in LBP : " +
          ((0.0 + localStats.convergenceItersLBP) / localStats.numVisitedDocs))
      println(key + " mentions " + " : percentage cases where LBP converged: " +
          (100.0 * (0.0 + localStats.converged) / localStats.numVisitedDocs))
      println(key + " mentions " + " : avg num candidates per mention: " +
          ((0.0 + localStats.avgNumCandPerMention) / localStats.numVisitedDocs))
          
      println
  }
  
  def print(full : Boolean) = {
    if (full) {
      for (key <- keyList) {
        if (stats.get(key).numVisitedDocs > 0) {
          printOneKey(key)
        }
      }
    } else {
      printOneKey("GLOBAL")
    }
  }
  
  def numDocs : Int = {
    val localStats = stats.get("GLOBAL")
    localStats.numVisitedDocs 
  }

  def numGroundTruthMention(key : String = "GLOBAL") : Int = {
    val localStats = stats.get("GLOBAL")
    localStats.numGroundTruthMentions  
  }
  
  def microF1 : Double = {
    val localStats = stats.get("GLOBAL")
    val precLoopy = localStats.microSumIntersectionLoopy / localStats.microSumSolutionLoopy
    val recallLoopy = localStats.microSumIntersectionLoopy / localStats.microSumGroundTruthLoopy
    return 100.0 * 2 * precLoopy * recallLoopy / (precLoopy + recallLoopy)
  }
  
  def macroF1 : Double = {
    val localStats = stats.get("GLOBAL")   
    val macroPrecLoopy = localStats.macroSumPrecisionLoopy / localStats.numVisitedDocs
    val macroRecallLoopy = localStats.macroSumRecallLoopy / localStats.numVisitedDocs  
    return 100.0 * 2 * macroPrecLoopy * macroRecallLoopy / (macroPrecLoopy + macroRecallLoopy)
  }
}