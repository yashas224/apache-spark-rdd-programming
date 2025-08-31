package org.example;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

/*
Class to read from input.txt and get the top 10 non boring [boringwords.txt] unique words
 */
public class SortingIssueDemo {

  public static void main(String[] args) throws IOException {
    Util util = new Util();
    SparkConf conf = new SparkConf().setAppName("CountNonBoringWords").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> lines = sc.textFile("src/main/resources/subtitles/input.txt");

    // not sorted

    JavaPairRDD rdd = lines.map(line -> line.replaceAll("[^a-zA-Z ]+", "").toLowerCase().trim())
       .filter(StringUtils::isNotBlank)
       .flatMap(str -> Arrays.asList(str.trim().split(" ")).iterator())
       .filter(word -> !util.isBoringWord(word) && StringUtils.isNotBlank(word))
       .mapToPair(word -> new Tuple2<>(word, 1))
       .reduceByKey((w1, w2) -> w1 + w2)
       .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
       .sortByKey(false);

    System.out.println("Partitions Count: " + rdd.getNumPartitions());

    rdd.foreach(t -> System.out.println(Thread.currentThread().getName() + "---" + t));

    // sorted
    // This is a wring approach to get the answer,
    // using coalesce(1) is WRONG as it wiuld get all the RDD's into a single partition and would not make sense if the data is huge

//    lines.map(line -> line.replaceAll("[^a-zA-Z ]+", "").toLowerCase().trim())
//       .filter(StringUtils::isNotBlank)
//       .flatMap(str -> Arrays.asList(str.trim().split(" ")).iterator())
//       .filter(word -> !util.isBoringWord(word) && StringUtils.isNotBlank(word))
//       .mapToPair(word -> new Tuple2<>(word, 1))
//       .reduceByKey((w1, w2) -> w1 + w2)
//       .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
//       .sortByKey(false)
//       .coalesce(1)
//       .foreach(t -> System.out.println(t));

    // correct explanation why forEach does not return RDD's in sorted order

    // forEach(function)
    /*
    The function is passed by the driver to each node in parallel
    The function is also executed against each partition  of the node in parallel.
    Its eventually multiple threads trying to execute the function on RDD of different partition on different nodes
    On local , its just two threads printing their RDD contents in two partitions [Partitions Count = 2]
     */
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
