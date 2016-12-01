package eval.aturk

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

object InterpretAmazonTurkResults {


  // Headers: 
  // "HITId","HITTypeId","Title","Description","Keywords",
  // "Reward","CreationTime","MaxAssignments","RequesterAnnotation",
  // "AssignmentDurationInSeconds","AutoApprovalDelayInSeconds","Expiration",
  // "NumberOfSimilarHITs","LifetimeInSeconds","AssignmentId","WorkerId",
  // "AssignmentStatus","AcceptTime","SubmitTime","AutoApprovalTime",
  // "ApprovalTime","RejectionTime","RequesterFeedback","WorkTimeInSeconds",
  // "LifetimeApprovalRate","Last30DaysApprovalRate","Last7DaysApprovalRate",
  // "Input.mentionid","Input.context","Input.googleinindex","Input.sourcea",
  // "Input.entitya","Input.paragrapha","Input.sourceb","Input.entityb",
  // "Input.paragraphb","Input.sourcec","Input.entityc","Input.paragraphc",
  // "Answer.Feedback","Answer.OtherOption","Answer.elanswer","Approve","Reject"

  def run() { 
    computeAverageWorkTimeInSeconds("results31oct/Batch_1705116_batch_results.csv")
    
    republishEmptyOtherAssignments("results31oct/Batch_1705116_batch_results.csv")
    

    //printWorkersStats()
  }
  
  
  def republishEmptyOtherAssignments(file : String) = {
    val fw = new java.io.FileWriter("results31oct/reject_and_republish.csv")
    
    
    fw.write("\"AssignmentId\",\"HITId\",\"Reject\"\n")
    val annotations = scala.io.Source.fromFile(file).getLines
    val headers = annotations.next.split(",")
    
    val lines = new ArrayBuffer[String]
    while (annotations.hasNext) {
      lines += annotations.next
    }
    
    for (line <- lines) {
      val ans = (line.split("\",\"")(headers.indexOf("\"Answer.elanswer\"")))
      if (ans.contains("other") &&   
          (line.split("\",\"")(headers.indexOf("\"Answer.OtherOption\""))).size < 2) {
        fw.write(
            line.split("\",\"")(headers.indexOf("\"AssignmentId\"")) + "," +
            (line.split("\",\"")(headers.indexOf("\"HITId\""))).substring(1) + "," +
            "Your work is rejected because you chose Other and" +
            " put the wrong Wikipedia page in the Other text box or left it " +
            " emtpy.\n")
      }
    }
    
    fw.close()
  }
  
  
  def computeAverageWorkTimeInSeconds(file : String) = {
    val annotations = scala.io.Source.fromFile(file).getLines
    val headers = annotations.next.split(",")
    
    val lines = new ArrayBuffer[String]
    while (annotations.hasNext) {
      lines += annotations.next
    }
    
    val i = headers.indexOf("\"WorkTimeInSeconds\"")
    
    var size = 0
    var totalTime = 0.0
    for (line <- lines) {
      size += 1
      val time = line.split("\",\"")(i).toInt
      totalTime += time
    }
    println("\nAvg WorkTimeInSeconds for " + file + " annotations = " + totalTime/size)
    
    
    ///////////////////// Workers stats //////////////////////////
    
    val annotPerWorker = lines.groupBy(line => line.split("\",\"")(headers.indexOf("\"WorkerId\"")))
    println("\n Worker Stats: ")
    
    var numqtotal = 0
    for ((workerID, listOfLines) <- annotPerWorker) {
      numqtotal += listOfLines.size
      var totalTime = 0.0
      var totalSpamRandom = 0
      var totalSpamCandidates = 0
      var totalEmptyOther = 0
      							
      var reject = "accepted"
      for (line <- listOfLines) {
        totalTime += line.split("\",\"")(headers.indexOf("\"WorkTimeInSeconds\"")).toInt        
        val ans = (line.split("\",\"")(headers.indexOf("\"Answer.elanswer\"")))
        
        if (ans.contains("other") &&
            (line.split("\",\"")(headers.indexOf("\"Answer.OtherOption\""))).size < 2) {
          totalEmptyOther += 1
        }
        totalSpamCandidates += (if (ans.contains("spam_candidates")) 1 else 0)
        totalSpamRandom += (if (ans.contains("spam_random")) 1 else 0)
        if (line.split(",")(headers.indexOf("\"AssignmentStatus\"")).contains("Rejected")) {
          reject = "REJECTED"
        }
      }

      println (" Worker: " + workerID + " ;num q ans " + listOfLines.size +
          " ;avg time: " + (totalTime/listOfLines.size).intValue() +
          " ;" + reject +
          " ;num spam_random= " + totalSpamRandom + 
          " ;num spam_candidates= " + totalSpamCandidates +
          " ;num empty other = " + totalEmptyOther)
    }
    
    println (" Total questions : " + numqtotal + "\n")
    
    
    ///////////////////// Questions stats //////////////////////////
    val annotPerMention = lines.groupBy(line => line.split("\",\"")(headers.indexOf("\"Input.mentionid\"")))
    
    val numAnswersTable = new scala.collection.mutable.HashMap[Int, Int]
    for ((mentionID, listOfLines) <- annotPerMention) {
      numAnswersTable += ((listOfLines.size, 1 + numAnswersTable.getOrElse(listOfLines.size, 0)))
    }
    var numDiffQuestions = 0
    for (k <- numAnswersTable.keySet.toList.sortWith(_ < _)) {
      val v = numAnswersTable.get(k).get
      println(k + " workers per question: " + v + " questions.")
      numDiffQuestions += v
    }
    
    println("Num different questions = " + numDiffQuestions + "\n")

    val answersMap = new scala.collection.mutable.HashMap[String, Int]
    val answersMapVote = new scala.collection.mutable.HashMap[String, Int]
    
    // Difference in num of workers that gave the top answer and the num of 
    // workers that gave any other answer.
    val differenceWithTheRest = 0

    for ((mentionID, listOfLines) <- annotPerMention) {
      val numAnswers = listOfLines.size
      if (numAnswers >= 3) { 
        val numAnswersMap = new scala.collection.mutable.HashMap[String, Int]
        for (line <- listOfLines) {
          val ans = (line.split("\",\"")(headers.indexOf("\"Answer.elanswer\"")))
          numAnswersMap += ((ans, 1 + numAnswersMap.getOrElse(ans, 0)))
        }
      
        var numG = 0
        var numA = 0
        var numL = 0
        var numO = 0
        for ((answer, count) <- numAnswersMap) {
          if (answer.contains("other")) numO += count
          if (answer.contains("google")) numG += count        
          if (answer.contains("argmax")) numA += count
          if (answer.contains("loopy")) numL += count
        }
        val key = "G:" +numG + " L:" + numL + " A:" + numA + " O:" + numO
        answersMap += ((key, 1 + answersMap.getOrElse(key, 0)))
              
        if (isTheBestBy("loopy", numAnswersMap, differenceWithTheRest)) {
          answersMapVote += (("loopy", 1 + answersMapVote.getOrElse("loopy", 0)))
        }
        if (isTheBestBy("google", numAnswersMap, differenceWithTheRest)) {
          answersMapVote += (("google", 1 + answersMapVote.getOrElse("google", 0)))
        }
        if (isTheBestBy("other", numAnswersMap, differenceWithTheRest)) {
          answersMapVote += (("other", 1 + answersMapVote.getOrElse("other", 0)))
        }
        if (isTheBestBy("argmax", numAnswersMap, differenceWithTheRest)) {
          answersMapVote += (("argmax", 1 + answersMapVote.getOrElse("argmax", 0)))
        }
      }
    }  
    
    println("Answer stats:")
    for ((k,v) <- answersMap) {
      println(k + " num questions: " + v)      
    }
    println
    println("Results for questions where top answer has strictly more than " +
        differenceWithTheRest + " difference from the rest answers:")
    for ((k,v) <- answersMapVote) {
      println("Majority vote for : " + k + " - num questions: " + v)      
    }
  }
   
  
  def isTheBestBy(ans : String, numAnswers : HashMap[String, Int], diff : Int) : Boolean = {
    var ansCount = 0
    for ((answer, count) <- numAnswers) {
      if (answer.contains(ans)) {
        ansCount += count
      }
    }
    for ((answer, count) <- numAnswers) {
      if (!answer.contains(ans) && (count >= ansCount - diff)) {
        return false
      }
    }

    return true
  }
  

}