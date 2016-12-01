//package eval.cweb
//
//import java.util.regex.Pattern
//import org.apache.commons.lang3.StringEscapeUtils
//import de.l3s.boilerpipe.extractors.KeepEverythingExtractor
//import de.l3s.boilerpipe.extractors.ArticleExtractor
//import org.archive.io.warc.WARCReaderFactory
//import scala.collection.JavaConversions._
//import org.apache.commons.io.IOUtils
//import java.util.ArrayList
//import index.AllIndexesBox
//import java.util.HashMap
//import loopybeliefpropagation.ScorerWeights
//import utils.Normalizer
//import scala.collection.mutable.ArrayBuffer
//import eval.Annotation
//import eval.VerifyEDLBPForListOfWords
//import index.MentionEntitiesFrequencyIndex
//import index.MentEntsFreqIndexWrapper
//import loopybeliefpropagation.RhoCache
//
///*
// * To run this: time scala -J-Xmx50g -J-Xms50g -classpath "lib/*"
// * EL_LBP_Spark extractCWDisagreementsForAmazonTurk | tee disagreements_amazon
// *
// * cat disagreements_amazon | grep '\[...\]' | tee disagreements_amazon_0000tw-00warc_ATformat
// */
// */
//object ExtractCWDisagreementsForAmazonTurk {
//
//  private def applyRegex(regex: Pattern, text: String, replacement: String): String = {
//    regex.matcher(text).replaceAll(replacement)
//  }
//
//  private def normalizeWhiteSpace(text: String): String = {
//    val postprocessingDeleteWhitespace = Pattern.compile("\\s+", Pattern.CASE_INSENSITIVE)
//    applyRegex(postprocessingDeleteWhitespace, text, " ").trim
//  }
//
//  // Should be called on text coming from HTML or from Google annotations.
//  def cleanString(text : String, fromHTML : Boolean) : String =   {
//    var res = text
//    if (fromHTML) {
//      res = StringEscapeUtils.unescapeHtml4(res)
//      res = StringEscapeUtils.unescapeHtml3(res)
//      res = StringEscapeUtils.unescapeXml(res)
//    }
//    res = KeepEverythingExtractor.INSTANCE.getText(text).replace('’', ''')
//    res = normalizeWhiteSpace(res)
//    res = res.map(c => if (Character.isSpaceChar(c)) ' ' else c)
//    res
//  }
//
//  def extractMentionTextFromOriginalPage(rawData : Array[Byte], elems : Array[String]) = {
//    val found = new String(rawData.slice(elems(3).toInt, elems(4).toInt), elems(1))
//    cleanString(found, true)
//  }
//
//  def extractAllTextFromOriginalPage(rawData : Array[Byte], elems : Array[String]) = {
//    val allText =
//      new String(rawData.slice(0, elems(3).toInt), elems(1)) +
//      "[{[{ " +
//      new String(rawData.slice(elems(3).toInt, elems(4).toInt), elems(1)) +
//      " ]}]}" +
//      new String(rawData.slice(elems(4).toInt ,rawData.length), elems(1))
//
//    cleanString(ArticleExtractor.getInstance().getText(allText), true)
//  }
//
//  // Extracts context of the webpage surrounding the mention which is signaled by [{[{ ... ]}]}
//  def extractContextFromOriginalPage(allText : String, elems : Array[String]) : String = {
//    var start = allText.indexOf("[{[{")
//    var end = allText.indexOf("]}]}")
//    if (start < 0 || end < 0) {
//      return null
//    }
//
//    var context =
//      allText.substring(
//          Math.max(0, allText.indexOf("[{[{") - 400),
//          Math.min(allText.length(), allText.indexOf("]}]}") + 400)
//      )
//    start = 0
//    end = context.length - 1
//    while (context(start) != '[' && !Character.isSpaceChar(context(start))) start += 1
//    while (context(end) != '}' && !Character.isSpaceChar(context(end))) end -= 1
//    context = "[...]" + context.substring(start, end + 1) + "[...]"
//
//    // Amazon turk doesn't allow comma and " in a CSV file
//    context = context.replaceAll("\"", "&ldquo;" )
//    context = context.replaceAll(",", "&#44;" )
//
//    start = context.indexOf("[{[{")
//    end = context.indexOf("]}]}")
//    if (start < 0 || end < 0) {
//      return null
//    }
//
//    context = context.substring(0, start) + "<font color=red size=3><span style=font-weight:bold>" +
//    		  context.substring(start + "[{[{".length(), end) + "</span></font>" +
//    		  context.substring(end + "]}]}".length(), context.length)
//    context
//  }
//
//
//  def loadWikiFirstParagraphIndex(allIndexesBox : AllIndexesBox) : HashMap[Int, String] = {
//    println("Loading wikipedia first paragraph index ... ")
//
//    val plainTextWikipediaIterator =
//      scala.io.Source.fromFile("/mnt/cw12/other-data/Wikipedia/WikipediaPlainText/textFromAllWikipedia2014Feb.txt").getLines
//    var index = new HashMap[Int, String]
//    var line = ""
//    while (plainTextWikipediaIterator.hasNext) {
//      line = plainTextWikipediaIterator.next
//      if (line.startsWith("<doc id=") && line.contains("url=\"http://en.wikipedia.org/wiki?curid=")) {
//        val id = line.substring(line.indexOf("/wiki?curid=") + "/wiki?curid=".length , line.indexOf("\" title=")).toInt
//        line = plainTextWikipediaIterator.next // Title line
//        val url = line
//
//        line = plainTextWikipediaIterator.next // Empty line
//        line = plainTextWikipediaIterator.next // First paragraph line or '</doc>' if empty doc.
//
//        // Amazon turk doesn't allow comma and " in a CSV file
//        line = line.replaceAll("\"", "&ldquo;")
//        line = line.replaceAll(",", "&#44;" )
//
//        index.put(id, line)
//      }
//    }
//
//    println("Done. Loaded wikipedia first paragraph index.")
//    index
//  }
//
//
//
//
//  def run(
//      shardIndex : Int,
//      allIndexesBox : AllIndexesBox,
//      priorConstant : Double,
//      max_product : Boolean) = {
//
//    val wikiFirstParIndex = loadWikiFirstParagraphIndex(allIndexesBox)
//
//    val annotations =
//      scala.io.Source.fromFile("/mnt/cw12/other-data/FreebaseAnnotationsOfClueweb12/ClueWeb12_00/0000tw/0000tw-0" + shardIndex + ".anns.tsv").getLines
//    val fn = "/mnt/cw12/cw-data-b-set/ClueWeb12_00/0000tw/0000tw-0" + shardIndex + ".warc"
//
//    var line : String = ""
//
//    var bad = 0
//    var total = 0
//
//    // Headers:
//    println("mentionid,context,googleinindex,sourcea,entitya,paragrapha,sourceb,entityb,paragraphb,sourcec,entityc,paragraphc")
//
//    // The file name identifies the ArchiveReader and indicates if it should be decompressed
//    val reader = WARCReaderFactory.get(new java.io.File(fn))
//    for (r <- reader.iterator() ) {
//      if (r.getHeader().getHeaderValue("WARC-Type") == "response") {
//        val cwid = r.getHeader.getHeaderValue("WARC-TREC-ID").toString()
//
//
//        val url = r.getHeader.getHeaderValue("WARC-Target-URI").toString()
//
//        val rawData = IOUtils.toByteArray(r, r.available());
//
//        val contentLen : Int = r.getHeader().getHeaderValue("Content-Length").toString.toInt
//        assert (rawData.length == contentLen)
//
//        while(line.split("\t").head < cwid) {
//          line = annotations.next
//        }
//
//        // Keep just lines corresponding to distinct mentions (we don't want the same
//        // mention to appear twice in a document).
//        var lines = scala.collection.mutable.HashMap.empty[String,String]
//        while(line.split("\t").head == cwid) {
//          val tkspan = line.split("\t")(2)
//          lines += (tkspan -> line)
//          line = annotations.next
//        }
//
//
//        var tkspans = new ArrayBuffer[String]
//        var entities = new ArrayBuffer[Int]
//        var offsets = new ArrayBuffer[Int]
//
//        for (line <- lines.values) {
//          val elems = line.split("\t")
//          if (elems.size != 8) {
//            throw new Exception("Clueweb annotation line is not well formated: " + line)
//          }
//          val freebaseId = elems(7).substring(elems(7).indexOf("/m/") + "/m/".length())
//          // Keep just ground truth mentions for which we have a corresponding Wikipedia entity.
//          if (allIndexesBox.freebaseToWikiIDIndex.containsKey(freebaseId)) {
//            tkspans += Normalizer.normalizeLowercase(elems(2))
//            offsets += elems(3).toInt
//            entities += allIndexesBox.freebaseToWikiIDIndex.get(freebaseId)
//          }
//        }
//
//        val mentEntsCache = new MentEntsFreqIndexWrapper(allIndexesBox, new RhoCache, tkspans.toArray, null, new ScorerWeights)
//
//        // RUN Loopy
//        var wrongAnnotLoopy : ArrayBuffer[Annotation] = null
//        var wrongAnnotArgmax : ArrayBuffer[Annotation] = null
//        if (tkspans.size < 800 && tkspans.size > 0)  { //HACK to make it take less time
//          val scorerWeights = new ScorerWeights(priorConstant,1,1,1,1,1)
//
//          // Run EL-LBP and ARGMAX:
//          val loopyRunner = new VerifyEDLBPForListOfWords(allIndexesBox, tkspans.toArray, entities.toArray, null, offsets.toArray, scorerWeights)
//          if (tkspans.size() > 0) {
//            // Process and run the EL-LBP algorithm.
//            val normalizedPosterior = true
//            val contexts = null
//            val verifierLoopy = loopyRunner
//            	.runEDLoopyOnOneInputList(max_product)
//            	.verifier
//
//            // Process and run ARGMAX version.
//            val verifierARGMAX = loopyRunner
//            	.runEDArgmaxOnOneInputList()
//
//            wrongAnnotLoopy = verifierLoopy.getWrongAnnotations
//            wrongAnnotArgmax = verifierARGMAX.getWrongAnnotations
//          }
//
//          println("\nFor file: " + cwid + "  ; num google annot = " + tkspans.size() +
//              " ; out of which " + wrongAnnotLoopy.size() + " different annot from loopy and " +
//              + wrongAnnotArgmax.size() + " different annot from ARGMAX. ")
//
//          // Compare Loopy results with Google annotations
//          for (a <- wrongAnnotLoopy) {
//            for (line <- lines.values) {
//              val elems = line.split("\t")
//
//              if (elems(3).toInt == a.getMention.getOffset &&
//                  Normalizer.normalizeLowercase(elems(2)) == Normalizer.normalizeLowercase(a.getMention.getNgram)) {
//
//                val mentionFoundInPage = extractMentionTextFromOriginalPage(rawData, elems)
//                val allText = extractAllTextFromOriginalPage(rawData, elems)
//                val context = extractContextFromOriginalPage(allText, elems)
//
//                if (context != null &&
//                    mentionFoundInPage.toLowerCase().replaceAll("\\.", "") ==
//                      cleanString(elems(2), false).toLowerCase().replaceAll("\\.", "") &&
//                    allText.indexOf("[{[{") >= 0 /* Make sure the annotation is not inside a non-English text */) {
//
//                  //println("CW ID: " + cwid)
//                  //println("Original URL: " + url)
//                  //println("Encoding: " + elems(1))
//                  //println("Mention text: " + cleanString(elems(2)))
//
//                  val freebaseID = elems(7).substring(elems(7).indexOf("/m/") + "/m/".length())
//                  val googleAnnotID = allIndexesBox.freebaseToWikiIDIndex.get(freebaseID)
//                  val loopyAnnotID = a.getEntity()
//                  var argmaxAnnot : Annotation = null
//                  for (b <- wrongAnnotArgmax) {
//                    if (b.getMention.getOffset() == a.getMention.getOffset && b.getMention.getNgram == a.getMention.getNgram) {
//                      argmaxAnnot = b
//                    }
//                  }
//                  val argmaxAnnotID : Int =
//                    if (argmaxAnnot == null)
//                      googleAnnotID
//                    else
//                      argmaxAnnot.getEntity
//
//                  if (googleAnnotID != loopyAnnotID &&
//                      !allIndexesBox.entIDToNameIndex.get(googleAnnotID).contains('\"') &&
//                      !allIndexesBox.entIDToNameIndex.get(googleAnnotID).contains(',') &&
//                      !allIndexesBox.entIDToNameIndex.get(loopyAnnotID).contains('\"') &&
//                      !allIndexesBox.entIDToNameIndex.get(loopyAnnotID).contains(',') &&
//                      !allIndexesBox.entIDToNameIndex.get(argmaxAnnotID).contains('\"') &&
//                      !allIndexesBox.entIDToNameIndex.get(argmaxAnnotID).contains(',') ) {
//
//                    val outputLineID = cwid + (Math.random() * 1000).toInt
//
//                    var source1 = "google"
//                    var wikipageID1 = googleAnnotID
//                    var source2 = "loopy"
//                    var wikipageID2 = loopyAnnotID
//                    var spamPair =
//                      getSpamEntityID(
//                          Normalizer.normalizeLowercase(elems(2)),
//                          wikipageID1,
//                          wikipageID2,
//                          mentEntsCache,
//                          allIndexesBox,
//                          wikiFirstParIndex)
//                    var source3 = spamPair._2
//                    var wikipageID3 = spamPair._1
//
//                    if (argmaxAnnotID == googleAnnotID) {
//                      source1 = "google_argmax"
//                      source2 = "loopy"
//                    } else if (argmaxAnnotID == loopyAnnotID) {
//                      source1 = "google"
//                      source2 = "loopy_argmax"
//                    } else if (argmaxAnnotID != loopyAnnotID && argmaxAnnotID != googleAnnotID) {
//                      source3 = "argmax"
//                      wikipageID3 = argmaxAnnotID
//                    }
//
//                    var options = new ArrayList[String]
//                    options.add(buildOneOptionString(wikipageID1, source1, allIndexesBox, wikiFirstParIndex))
//                    options.add(buildOneOptionString(wikipageID2, source2, allIndexesBox, wikiFirstParIndex))
//                    options.add(buildOneOptionString(wikipageID3, source3, allIndexesBox, wikiFirstParIndex))
//
//                    var optionsString = ""
//                    new scala.util.Random().shuffle(0 to 2) foreach { i => optionsString += options(i) }
//
//                    // Check if google entity is in our mention frequency index.
//                    var googleinindex = "ok"
//                    if (!mentEntsCache.getCandidateEntities(Normalizer.normalizeLowercase(elems(2)))
//                        .contains(googleAnnotID)) {
//                      googleinindex = "notgoogleinindex_" +
//                    		  mentEntsCache.getCandidateEntitiesCount(Normalizer.normalizeLowercase(elems(2)))
//                    }
//
//                    println(outputLineID + "," + context + "," + googleinindex + "," + optionsString)
//                  }
//                }
//              }
//            }
//          }
//        }
//      }
//    }
//    println("Done. Num Google annots: " + total)
//
//  }
//
//
//  def getRandomWikipage(
//      allIndexesBox : AllIndexesBox,
//      wikiFirstParIndex: java.util.HashMap[Int,String]) : Int = {
//    var id :Int = (Math.random() * 4000000).toInt
//    while (!allIndexesBox.entIDToNameIndex.containsKey(id) ||
//        !wikiFirstParIndex.containsKey(id)) {
//      id = (Math.random() * 4000000).toInt
//    }
//    id
//  }
//
//  def getSpamEntityID(
//      mention : String,
//      id1 : Int,
//      id2 : Int,
//      mentEntsCache : MentEntsFreqIndexWrapper,
//      allIndexesBox : AllIndexesBox,
//      wikiFirstParIndex: java.util.HashMap[Int,String]) : (Int,String) = {
//
//    if (!mentEntsCache.containsMention(mention) || Math.random() < 0.5) {
//      return (getRandomWikipage(allIndexesBox, wikiFirstParIndex), "spam_random")
//    }
//    val candidates = mentEntsCache.getCandidateEntities(mention)
//    var spamID = 1000
//    var score : Double = 10000000
//    for (cand <- candidates) {
//      val sc = mentEntsCache.getCandidateProbability(mention, cand.intValue())
//      if (cand.intValue() != id1 && cand.intValue() != id2 && sc < score) {
//        score = sc
//        spamID = cand.intValue()
//      }
//    }
//
//    if (score == 10000000 || candidates.size < 5) {
//      println("Cand index size < 2 for mention = " + mention + " " +
//          mentEntsCache.getCandidateEntitiesCount(mention))
//      return (getRandomWikipage(allIndexesBox, wikiFirstParIndex), "spam_random")
//    }
//
//    (spamID, "spam_candidates")
//  }
//
//
//  def buildOneOptionString(
//      wikipageID : Int,
//      sourcesString : String,
//      allIndexesBox : AllIndexesBox,
//      wikiFirstParIndex: java.util.HashMap[Int,String]) : String = {
//
//    sourcesString + "," +
//    "http://en.wikipedia.org/wiki/" +
//    allIndexesBox.entIDToNameIndex.get(wikipageID).replace(' ', '_').replaceAll(",", "&#44;" ).replaceAll("\"", "&ldquo;") +
//    "," + wikiFirstParIndex.get(wikipageID) + ","
//  }
//
//}