package com.blueyonder.parquetdatamultiplier;

public class WrodReplacer {
  
  public static void main(String[] args) {
    String test = "Let's meet l8r 2nite?";
    wordReplacer(test);
  }
  
  public static void wordReplacer(String test) {
     String str = test.replaceAll("[a-zA-Z]+", "*");
     System.out.print(str);
  }

}
