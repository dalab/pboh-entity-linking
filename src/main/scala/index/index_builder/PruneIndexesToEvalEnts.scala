package index.index_builder

import gnu.trove.set.hash.TIntHashSet

object PruneIndexesToEvalEnts {
  def run(args: Array[String]) : Unit = {
    if (args.length < 1) {
      System.err.println("Usage: input_file")
      System.exit(1)
    }
    
    val ents_file = "/mnt/cw12/Octavian/el-lbp-marina/marinah/WikificationACL2011Data/entities_in_data"
    val ents = new TIntHashSet
    for (l <- scala.io.Source.fromFile(ents_file).getLines) {
      if (l.startsWith("####ENT")) {
        val tokens = l.split("\t")
        ents.add(tokens(1).toInt)
      }
    }
    println("Num ents = " + ents.size())
    
    val fw = new java.io.FileWriter(args(0) + "_prunned")
    val lines = scala.io.Source.fromFile(args(0)).getLines
   
    for (l <- lines) {
      // val tokens = l.split(Array('\t', ' ', ','))
      val tokens = l.split(Array('\t')).slice(1,2)
      
      var good = false
      for (t <- tokens) {
        if (t.length() > 0 && t.length() <= 9 && t.forall(_.isDigit) && ents.contains(t.toInt)) {
          good = true
        }
      }
      if (good) {
        fw.write(l + "\n")
      }
    }
   
    fw.close
  }
}