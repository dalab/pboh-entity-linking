package utils

import java.net.URLDecoder;
import org.apache.commons.lang3.StringEscapeUtils;


object Normalizer {

  def normalizeLowercase(s : String) : String = {
    return s.toLowerCase();
  }
 

  // Input should be the Wikipedia title page without the prefix http://en.wikipedia.org/wiki/
  def processTargetLink(input2 : String) : String = {
    var input = StringEscapeUtils.unescapeHtml4(StringEscapeUtils.unescapeHtml4(input2))
    
    input = input.replace("_", " ").trim();
    input = capitalizeFirstLetter(input);
    
    if (!input.contains("%")) {
      return input;
    }

    val decodedInput = URLDecoder.decode("http://a.a/" + input, "UTF-8").substring(11);
    return decodedInput.trim();
  }  
  
  def capitalizeFirstLetter(original : String) : String = {
    if(original.length() == 0) {
      return original;
    }
    return original.substring(0, 1).toUpperCase() + original.substring(1);
  }  
}