package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class DiscLoad {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("apache-spark-starter").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    System.out.println("------------------------------------------------ Reading from Disc ------------------------------------------------");
    JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

    initialRdd.flatMap(str -> Arrays.asList(str.split(" ")).iterator())
       .foreach(str -> System.out.println(str));

    sc.close();
  }
}
