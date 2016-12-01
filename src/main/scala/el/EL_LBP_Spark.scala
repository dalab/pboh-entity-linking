package el

import index.AllIndexesBox
import learning.Learning
import learning.FewParamsLearning
import console_el.ConsoleEntityLinking
import eval.cweb.RunLoopyAgainstAllGoogleClweb
import eval.SplitFileIntoDevValidationTest
import context.OneDocPerLine
import context.EntityWordsProbs
import index.index_builder._
import index.WordFreqDict
import index.EntityCooccurrenceIndex
import context.StopWords
import context.TextSplittingInWords
import learning.ExtractWikiAnchorsFromWikipedia
import context.WordFreqPerCorpus
import md.Mention
import scala.collection.mutable.ArrayBuffer

object EL_LBP_Spark  {
  
  def main(args: Array[String]) {
    val tailArgs = args.tail
    val program = args.head
    
    if (program == "interpretAmazonTurkResults") {
      System.err.println(" Program : interpret Amazon Turk Results")
      eval.aturk.InterpretAmazonTurkResults.run
      
    } else if (program == "countNumDistinctPairs") {
     val wikipEntityCooccurrIndex = new EntityCooccurrenceIndex("wikipedia_index")
     wikipEntityCooccurrIndex.computeNumDistinctPairs("/mnt/cw12/Octavian/el-lbp-marina/marinah/wikipedia/anchorsListFromEachWikiPage.txt_dev_index", true);     

    } else if (program == "createCrosswikisIndex") {
      System.err.println(" Program : create Crosswikis Index")      
      CreateCrosswikisMentionFreqIndex.outputEnglishCrosswikisData(tailArgs)

    } else if (program == "lowercaseCrosswikisMentionFreqIndex") {
      System.err.println(" Program : lowercase Crosswikis Mention Freq Index")
      LowercaseCrosswikisMentionFreqIndex.outputPrunnedIndex(tailArgs, true, true)

    } else if (program == "runLoopyAgainstAllGoogleClweb") {
      System.err.println(" Program : run Loopy Against All Google Clweb")  
      if (tailArgs.size < 1) {
        System.err.println(" [ERROR] Please specify a parameter either 'max-product' or 'sum-product'.")
        System.exit(1)
      }      
      val max_product = if (tailArgs(0) == "max-product") true else false      
      RunLoopyAgainstAllGoogleClweb.run(tailArgs, 0.6, max_product)

    } else if (program == "getFreqPairOfEntsForLearning") {
      System.err.println(" Program : get Freq Pair Of Ents For Learning")
      Learning.getFreqPairsOfEnts(tailArgs)

    } else if (program == "extractWikipAnchors") {
      System.err.println(" Program : extract Wikip Anchors")      
      ExtractWikiAnchorsFromWikipedia.run(tailArgs)

    } else if (program == "PruneIndexesToEvalEnts") {
      System.err.println(" Program : Prune Indexes To Eval Ents")      
      PruneIndexesToEvalEnts.run(tailArgs)
      
    } else if (program == "EntityWordsProbs") {
      System.err.println(" Program : Entity Words Probs p(w|e)")      
      EntityWordsProbs.run()    
      
    } else if (program == "WordFreqPerCorpus") {
      System.err.println(" Program : Word Freq Per Corpus")      
      WordFreqPerCorpus.run(tailArgs)    
      
    } else if (program == "oneDocPerLine") {
      System.err.println(" Program : One Doc Per Line")      
      OneDocPerLine.run(tailArgs)    
      
    } else if (program == "splitFileIntoDevValidationTest") {
      System.err.println(" Program : Split File Into Dev Validation Test")      
      SplitFileIntoDevValidationTest.run(tailArgs)    

    } else if (program == "createWikilinksAnchorIndex") {
      System.err.println(" Program : Create Wikilinks Anchor Index")      
      CreateWikilinksAnchorIndex.run(tailArgs)

    } else if (program == "createFreebaseAnchorIndex") {
      System.err.println(" Program : Create Freebase Anchor Index")      
      CreateFreebaseAnchorIndex.run(tailArgs)

    } else if (program == "consoleEntityLinking") {
      System.err.println("Program : console Entity Linking")      
      if (tailArgs.size < 2) {
        System.err.println(" [ERROR] Please specify a parameter either 'max-product' or 'sum-product'.")
        System.exit(1)
      }
      val max_product = if (tailArgs(1) == "max-product") true else false

      val console = new ConsoleEntityLinking
      console.consoleJustOutput(max_product)

    } else if (program == "learnFewParams") {
      System.err.println(" Program : learn Few Loopy Params")
      if (tailArgs.size < 2) {
        System.err.println(" [ERROR] Please specify a parameter either 'max-product' or 'sum-product'.")
        System.exit(1)
      }
      val max_product = if (tailArgs(1) == "max-product") true else false
      
      FewParamsLearning.learn(dataPath = tailArgs(0), max_product)

    } else if (program == "learnLoopyParams") {
      System.err.println(" Program : learn Loopy Params")
      if (tailArgs.size < 2) {
        System.err.println(" [ERROR] Please specify a parameter either 'max-product' or 'sum-product'.")
        System.exit(1)
      }
      val max_product = if (tailArgs(1) == "max-product") true else false
      Learning.learn(dataPath = tailArgs(0), max_product)      
    }   
  }
}
