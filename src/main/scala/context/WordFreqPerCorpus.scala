package context
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.MutableList


/*
 * MASTER=spark://dco-node040.dco.ethz.ch:7077 
 * /mnt/cw12/Octavian/spark-1.1.1-bin-hadoop2.4/bin/spark-submit --class
 *  "EL_LBP_Spark" --jars lib/el_lbp_clueweb_list_words_2.10-1.0.jar,
 *  lib/trove-3.1a1.jar,lib/spymemcached-2.10.3.jar lib/el-lbp.jar 
 *  WordFreqPerCorpus > >(tee out.log) 2> >(tee err.log >&2)
 */
object WordFreqPerCorpus {
  
  val totalSumFreqs : Long = 822112954L
  val vocabularySize = 582355
  
  def run(args: Array[String]) : Unit = {

    val input = "/mnt/cw12/other-data/Wikipedia/WikipediaPlainText/textFromAllWikipedia2014Feb.txt_one_doc_per_line"
    val output = "/mnt/cw12/other-data/Wikipedia/WikipediaPlainText/textFromAllWikipedia2014Feb.txt_word_frequencies"
    
    val conf = new SparkConf().setAppName("EL LBP SPARK")
    conf.set("spark.cores.max", "128")
    conf.set("spark.akka.frameSize", "1024")
    conf.set("spark.executor.memory", "100g")
    conf.set("spark.shuffle.file.buffer.kb", "1000")
    
    val sc = new SparkContext(conf)
    
    val sw = new StopWords
    val rdd = sc.textFile(input)
    		 .flatMap(line => TextSplittingInWords.splitDocInWords(line.substring(line.indexOf("\">##\t\t\t") + "\">##\t\t\t".length()), sw))
             .map(word => (word, 1))
             .reduceByKey(_ + _)
             .filter({case(w,count) => (count >= 20)})
     
    val totalSumFreqs = rdd.map({case(w,count) => count.toLong}).reduce(_ + _)
    
    val fw = new java.io.FileWriter(output)
    
    rdd.collect.zipWithIndex
    		   .foreach({case ((word,count), index) => fw.write(index + "\t" + word + "\t" + count + "\n")})
    		  
    fw.flush()
    fw.close()
    sc.stop()    
    
    println("\n\nDone word count . Sum frequencies words with freq >= 20 = " + totalSumFreqs)
 
  }
}