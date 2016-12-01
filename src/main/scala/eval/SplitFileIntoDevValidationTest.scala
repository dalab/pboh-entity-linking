package eval

// Output file is in the same directory, with names ending in "_dev_index", "_dev_learning", "_val", "_test"
object SplitFileIntoDevValidationTest {
  def run(args: Array[String]) : Unit = {
    if (args.length < 1) {
      System.err.println("Usage: input_file")
      System.exit(1)
    }
    
    val fw_dev_index = new java.io.FileWriter(args(0) + "_dev_index")
    val fw_dev_learning = new java.io.FileWriter(args(0) + "_dev_learning")
    val fw_val = new java.io.FileWriter(args(0) + "_val")
    val fw_test = new java.io.FileWriter(args(0) + "_test")
    
    val lines = scala.io.Source.fromFile(args(0)).getLines
    
    val total = lines.size
    println("Total lines = " + total)
    
    for (l <- scala.io.Source.fromFile(args(0)).getLines) {
      val x = Math.random()
      if (x < (1000.0 / total)) {
        fw_val.write(l + "\n") /// 3K for validation set
      } else if (x < (11000.0 / total)) {
        fw_test.write(l + "\n") /// 10K for test set
      } else {
        val y = Math.random()
        if (y <= 0.0012) {
          fw_dev_learning.write(l + "\n") // 0.12% for learning params
        } else {
          fw_dev_index.write(l + "\n")    // the rest for building co-occurrence indexes
        }
      }
    }
    
    fw_dev_learning.close
    fw_dev_index.close
    fw_val.close
    fw_test.close
  }
  
}