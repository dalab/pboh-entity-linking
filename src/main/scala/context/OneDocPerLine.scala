package context

/*
 * Converts the doc textFromAllWikipedia2014Feb.txt into a new doc 
 * with one document representation per line.
 */
object OneDocPerLine {
  def run(args: Array[String]) : Unit = {
    if (args.length < 1) {
      System.err.println("Usage: input_file")
      System.exit(1)
    }
    
    val out = new java.io.FileWriter(args(0) + "_one_doc_per_line")
    
    val lines = scala.io.Source.fromFile(args(0)).getLines
    
    val total = lines.size
    println("Total lines = " + total)
    
    var docString : StringBuilder = new StringBuilder("")
    var title = ""
      
    for (line <- scala.io.Source.fromFile(args(0)).getLines) {
      if (line.startsWith("<doc id=")) {
        if (docString.toString != "") {
          out.write(docString.toString + "\n")
        }
        docString = new StringBuilder("")
        docString.append(line.trim() + "##\t\t\t")
        title = line.substring(line.indexOf(" title=\"") + " title=\"".length(), line.indexOf("\">"))
      } else {
        if (line.trim() != "" && line.trim() != title) {
          docString.append(line.trim() + " ")
        }
      }
    }
    
    out.close
  }
}