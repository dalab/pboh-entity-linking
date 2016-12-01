package utils

object Utils {
  def extractWikipURL(wikipUrlFromFreebase : String) : String = {
    var wikipEntity = wikipUrlFromFreebase.substring(
        wikipUrlFromFreebase.indexOf("wikipedia/en/") + "wikipedia/en/".length());
    wikipEntity = wikipEntity.substring(0, wikipEntity.length() - 2);
		
    val sb = new StringBuilder();
    
    var i = 0;
    while (i < wikipEntity.length()) {
      if (wikipEntity.charAt(i) != '$') {
        sb.append(wikipEntity.charAt(i));
      } else {
        var ascii = "";
        ascii += wikipEntity.charAt(i);
        i += 1;
        ascii += wikipEntity.charAt(i);
        i += 1;
        ascii += wikipEntity.charAt(i);
        i += 1;
        ascii += wikipEntity.charAt(i);
        i += 1;

        sb.append(Integer.parseInt(ascii, 16).toChar);
      }
    }
    return sb.toString().replace('_', ' ');
  }
  
  
  
////////////////////////////////////////  
  
  
  def compressTwoInts(a : Int, b : Int) : Long = {
    ((a.toLong) << 32) | (b & 0xffffffffL);
  }
  
  def decompressOneLong(l : Long) : (Int, Int) = {
    ((l >> 32).toInt, l.toInt)
  }
  
  def unitTestCompress() = {
    val x = (Math.random() * 1000000000).toInt
    val y = (Math.random() * 1000000000).toInt
    val z = decompressOneLong(compressTwoInts(x, y))
    assert(x == z._1 && y == z._2 )
  }
  
  def unitTestCompressFull() = {
    val x = List.range(1, 5).foreach(a => unitTestCompress)
  }  
}
