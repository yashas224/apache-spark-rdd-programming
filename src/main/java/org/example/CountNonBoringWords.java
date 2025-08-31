package org.example;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/*
Class to read from input.txt and get the top 10 non boring [boringwords.txt] unique words
 */
public class CountNonBoringWords {

  public static void main(String[] args) throws IOException, InterruptedException {
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    Util util = new Util();
    SparkConf conf = new SparkConf().setAppName("CountNonBoringWords").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    String currentWorkingDirectory = System.getProperty("user.dir");
    JavaRDD<String> lines = sc.textFile(currentWorkingDirectory.concat("/src/main/resources/subtitles/input.txt"));

//    lines
//       .filter(str -> !str.contains("-->"))
//       .flatMap(str -> Arrays.asList(str.split(" ")).iterator())
//       .filter(word -> StringUtil.isNotBlank(word) && !util.isBoringWord(word) && !isNumber(word))
//       .mapToPair(word -> new Tuple2<>(word, 1))
//       .reduceByKey((w1, w2) -> w1 + w2)
//       .sortByKey()
//       .take(10)
//       .forEach(word -> System.out.println(word));

    System.out.println("Final approach");

    List test = lines
       .map(line -> line.replaceAll("[^a-zA-Z ]+", "").toLowerCase().trim())
       .filter(StringUtils::isNotBlank)
       .flatMap(str -> Arrays.asList(str.trim().split(" ")).iterator())
       .filter(word -> !util.isBoringWord(word) && StringUtils.isNotBlank(word))
       .mapToPair(word -> new Tuple2<>(word, 1))
       .reduceByKey((w1, w2) -> w1 + w2)
       .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
       .sortByKey(false)
       .take(10);

    test.forEach(str -> System.out.println(str));

    Thread newThread = new Thread(() -> {
      while(true) {
      }
    });

    newThread.start();

    newThread.join(10000000);
  }

  private static boolean isNumber(String str) {
    try {
      Integer.parseInt(str);
      return true;
    } catch(Exception e) {
      return false;
    }
  }
}
