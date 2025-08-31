package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkIntro {
  public static void main(String[] args) {
    List<Integer> list = new ArrayList<>();
    for(int i = 0; i < 10; i++) {
      list.add(i);
    }
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("apache-spark-starter").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<Integer> rdd = sc.parallelize(list, 5);

    rdd.foreach(t -> System.out.println(t));

    System.out.println("Available CPU cores: " + Runtime.getRuntime().availableProcessors());

    Integer result = rdd.reduce((va1, val2) -> {
      System.out.println("Thread " + Thread.currentThread().getName());
      return Integer.sum(va1, val2);
    });

    System.out.println("Result :" + result);

    JavaRDD<Double> doubleJavaRDD = rdd.map(nos -> Math.sqrt(nos));
    System.out.println("------------------------------------------------SQRT------------------------------------------------");
    doubleJavaRDD.foreach(t -> System.out.println(t));

    System.out.println("------------------------------------------------count------------------------------------------------");
    System.out.println(doubleJavaRDD.count());

    System.out.println("------------------------------------------------Map & Reduce Sample------------------------------------------------");
    int countFromMapReduce = doubleJavaRDD.map(n1 -> 1).reduce((nos1, nos2) -> nos2 + nos1);
    System.out.println("MapReduce Count :" + countFromMapReduce);

    System.out.println("------------------------------------------------Object class ------------------------------------------------");

    JavaRDD<Integer> mainInteger = sc.parallelize(list, 5);
    JavaRDD<Node> nodeRdd = mainInteger.map(Node::new);
    nodeRdd.foreach(i -> System.out.println("NOS :" + i.nos + "--" + "SQRT :" + i.sqrt));

    System.out.println("------------------------------------------------Tuple2 ------------------------------------------------");

    JavaRDD<Tuple2<Integer, Double>> tuple2JavaRDD = mainInteger.map(i -> new Tuple2<>(1, Math.sqrt(i)));
    tuple2JavaRDD.foreach(t -> System.out.println("Nos :" + t._1() + "-- Sqrt :" + t._2()));

    System.out.println("------------------------------------------------JavaPairRDD ------------------------------------------------");

    List<String> logs = new ArrayList<>();
    logs.add("WARN: Tuesday 4 September 0405");
    logs.add("WARN: Tuesday 4 September 0405");
    logs.add("FATAL: Monday 3 September 0300");
    logs.add("ERROR: Wednesday 5 September 0505");
    logs.add("ERROR: Wednesday 5 September 2021");
    logs.add("INFO: Thursday 6 September 2025");

    JavaRDD<String> mainLog = sc.parallelize(logs);
    JavaPairRDD<String, String> pairRDD = mainLog.mapToPair(t -> {
      System.out.println("Thread" + Thread.currentThread().getName());
      String[] arr = t.split(":");
      return new Tuple2<>(arr[0], arr[1]);
    });

    pairRDD.foreach(pair -> System.out.println(Thread.currentThread().getName() + "--" + "key : " + pair._1() + ", Value : " + pair._2()));

    System.out.println("------------------------------------------------Count PairRdd ------------------------------------------------");

    JavaPairRDD<String, Long> countPairRdd = mainLog.mapToPair(t -> new Tuple2<>(t.split(":")[0], 1L));
    JavaPairRDD<String, Long> sumRdd = countPairRdd.reduceByKey(Long::sum);
    sumRdd.foreach(t -> System.out.println("key : " + t._1() + ", total count : " + t._2()));

    System.out.println("------------------------------------------------ PairRdd group By ------------------------------------------------");
    countPairRdd.groupByKey().foreach(t -> System.out.println("key : " + t._1() + ", total count : " + Iterables.size(t._2)));

    System.out.println("------------------------------------------------ Flat Map ------------------------------------------------");

    sc.parallelize(logs)
       .flatMap(str -> Arrays.asList(str.split(" ")).iterator())
       .filter(str -> !str.isEmpty())
       .foreach(word -> System.out.println(word));

    System.out.println("------------------------------------------------ Flat Map , FIlter ------------------------------------------------");
    sc.parallelize(logs)
       .flatMap(str -> Arrays.asList(str.split(" ")).iterator())
       .filter(str -> str.length() > 1)
       .foreach(word -> System.out.println(word));


    sc.close();
  }
}

class Node {
  int nos;
  double sqrt;

  public Node(int nos) {
    this.nos = nos;
    this.sqrt = Math.sqrt(nos);
  }
}
