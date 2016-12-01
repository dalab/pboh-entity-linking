package index.index_builder

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/*
 * Lowercase all token spans in Crosswikis MentionEntitiesFrequencyIndex 
 * and/or prunning to keep top 50 candidates for each name. 
 * 
 * Run with:
 * rm -Rf /mnt/cw12/Octavian/thetas; MASTER=spark://dco-node040.dco.ethz.ch:7077
 *  ~/spark/bin/spark-submit --class "EL_LBP_Spark"  --executor-memory 100G 
 *  --total-executor-cores 128 --jars lib/el_lbp_clueweb_list_words_2.10-1.0.jar
 *  lib/el-lbp.jar lowercaseCrosswikisMentionFreqIndex
 *  /mnt/cw12/Octavian/el-lbp-marina/marinah/wikipedia/mek-top-freq-crosswikis.txt
 *   /mnt/cw12/Octavian/thetas
 * 
 */

object LowercaseCrosswikisMentionFreqIndex {
  
  def collapseLinesFromTheSameLowercaseName(
      iter: Iterator[(String, Iterable[String])], 
      justTopX : Boolean) 
  : Iterator[String] = {

    var list : List[String] = List.empty
    for ((lowerCaseName, lines) <- iter) {
      var numEntWithName = 0
      val cand = new collection.mutable.HashMap[Int,Int]     
      for (line <- lines) {
        val terms = line.split("\t")
        numEntWithName += terms(3).toInt
        for (i <- 4 until terms.size) {
          val t = terms(i).split(",")
          cand += ((t(0).toInt, t(1).toInt + cand.getOrElse(t(0).toInt, 0)))
        }
      }
      val l = cand.toList.sortBy(_._2).reverse
      var outputLine = lowerCaseName + "\t10\t10\t" + numEntWithName
      
      var num = 0
      for (c <- l) {
        num += 1
        if (!justTopX || num <= 64) { // Top 64.
          outputLine += "\t" + c._1 + "," + c._2
        }
      }
      list = list.::(outputLine)
    }    
    list.iterator
  }  
  
  def outputPrunnedIndex(
      args: Array[String], 
      toLowerCase : Boolean,
      justTop50 : Boolean) : Unit = {
    
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
    
    conf.set("spark.broadcast.factory",
        "org.apache.spark.broadcast.TorrentBroadcastFactory")
    
    val sc = new SparkContext(conf)
    
    val numWikisNotInRedirOrNameToIDIndexes = sc.accumulator(0)  
    val numWikisScanned = sc.accumulator(0)  
    
    sc.textFile(args(0))
      .map(line => {
        val name = line.split("\t").head
        if (toLowerCase) {
          (name.toLowerCase, line)          
        } else {
          (name , line)
        }
      })
      .groupByKey(128)  // Group by name    
      .mapPartitions(iter => collapseLinesFromTheSameLowercaseName(iter, justTop50))
      .saveAsTextFile(args(1))
    
    println("=================================================================")
    println("================== Finished writing Crosswikis file =============")
    println("=================================================================")

    sc.stop()    
  }
}