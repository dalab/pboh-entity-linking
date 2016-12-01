package index.index_builder

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import index.AllIndexesBox
import org.apache.spark.Accumulator
import utils.Normalizer
/*
 * Converts Crosswikis dict into a MentionEntitiesFrequencyIndex format.
 * 
 * Run with:
 * rm -Rf /mnt/cw12/Octavian/thetas;
 * MASTER=spark://dco-node040.dco.ethz.ch:7077 ~/spark/bin/spark-submit
 * --class "EL_LBP_Spark"  --executor-memory 100G --total-executor-cores 128
 * --jars lib/el_lbp_clueweb_list_words_2.10-1.0.jar  lib/el-lbp.jar
 * createCrosswikisIndex /mnt/cw12/other-data/mergedCrosswikis/dict.merged
 * /mnt/cw12/Octavian/thetas
 * 
 */

object AllIndexesWrapper {
  private val indexesToLoad = "entNameToIDIndex, redirectIndex"
  private val allIndexes = new AllIndexesBox(indexesToLoad)

  def getAllIndexesBox : AllIndexesBox = allIndexes
}


object CreateCrosswikisMentionFreqIndex {
  
  def parseEntLinesAndOutputAsAString(
      iter : Iterator[(String, Iterable[String])],
      allIndexesBox : AllIndexesBox,
      numWikisNotInRedirOrNameToIDIndexes : Accumulator[Int],
      numWikisScanned : Accumulator[Int]) : Iterator[String] = {

    var result = ""
  
    for ((name, linesIterator) <- iter) {
      var resultForThisName = ""
      var numEntsWithThisName = 0

      for (line <- linesIterator) {
        // line : <string><tab><cprob><space><url>[<space><score>]*
        val terms = line.split("\t")
        if (terms.size != 2) {
          throw new Exception("Crosswikis line is not well formated: " + line)
        }

        val parseVec = terms(1).split(" ")
        
        // Conversion: Wiki url <-> Wiki id
        val ent = parseVec(1)
        val normalizedTarget = Normalizer.processTargetLink(ent); //check if it exists
        val canonicalTarget = allIndexesBox.redirectIndex.getCanonicalURL(normalizedTarget);
        val entityId = allIndexesBox.entNameToIDIndex.getTitleId(canonicalTarget);
        if (entityId < 0) {
          numWikisNotInRedirOrNameToIDIndexes += 1
        }
        numWikisScanned += 1
        
        var nameEntFreq : Int = 0 // Compute from W, w and w' values.
        for (i <- 2 until parseVec.size) {
          val reducedSerialScore = parseVec(i).split(":")
          if (reducedSerialScore.head != "Wx") {
            nameEntFreq += reducedSerialScore.last.split("/").head.toInt
          }
        }
        
        if (entityId > 0 && // it can be -1 if the entity is not in the entNameToIdIndex
            nameEntFreq >= 5) {
          numEntsWithThisName += nameEntFreq
          resultForThisName += "\t" + entityId + "," + nameEntFreq
        }
      }
          
      if (numEntsWithThisName >= 5) {
        if (result.size > 0) {
          result += "\n"
        }
        result += 
          name + "\t10\t10\t" + numEntsWithThisName + resultForThisName
      }
    }

    var list : List[String] = List(result)
    if (result.size == 0) {
      list = List.empty
    }
    list.iterator
  }  
  
  def outputEnglishCrosswikisData(args: Array[String]) : Unit = {
    if (args.length < 2) {
      System.err.println("Usage: input_file output_file")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("EL LBP SPARK")
    conf.set("spark.cores.max", "128")
    
    conf.set("spark.akka.frameSize", "1024")
    conf.set("spark.executor.memory", "100g")
    conf.set("spark.shuffle.file.buffer.kb", "1000")
    conf.set("spark.broadcast.compress", "true")
    
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")
    
    val sc = new SparkContext(conf)
    
    val numWikisNotInRedirOrNameToIDIndexes = sc.accumulator(0)  
    val numWikisScanned = sc.accumulator(0)  
    
    sc.textFile(args(0))
      .map(line => {
        val name = line.split("\t").head;
        (name , line)
      })
      .groupByKey(128)  // Group by name
      .mapPartitions(linesIter => 
        parseEntLinesAndOutputAsAString(
            linesIter,
            AllIndexesWrapper.getAllIndexesBox,
            numWikisNotInRedirOrNameToIDIndexes,
            numWikisScanned))
      .saveAsTextFile(args(1))
    
    println("==========================================================================")
    println("================== Finished writing Crosswikis file =================================")
    println("==========================================================================")
    
    // ====== numWikisNotInRedirOrNameToIDIndexes = 11435636
    // ====== numWikisScanned = 140632267
    println("====== numWikisNotInRedirOrNameToIDIndexes = " + numWikisNotInRedirOrNameToIDIndexes.value)
    println("====== numWikisScanned = " + numWikisScanned)
    sc.stop()    
    
    
  }
}