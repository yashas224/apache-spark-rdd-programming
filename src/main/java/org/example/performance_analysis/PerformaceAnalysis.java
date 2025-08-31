package org.example.performance_analysis;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.spark_project.guava.collect.Iterables;
import scala.Tuple2;

public class PerformaceAnalysis {
  public static void main(String[] args) throws InterruptedException {
    Logger.getLogger("org.apache").setLevel(Level.WARN);
    SparkConf conf = new SparkConf().setAppName("performance-analysis").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    String currentWorkingDirectory = System.getProperty("user.dir");
    JavaRDD<String> rdd = sc.textFile(currentWorkingDirectory.concat("/src/main/resources/bigLog.txt"));

    System.out.println("Initial Partitions : " + rdd.getNumPartitions());

    JavaPairRDD<String, String> rddPair1 = rdd.mapToPair(t -> {
      String[] strArray = t.split(":");
      return new Tuple2<>(strArray[0], strArray[1]);
    });

    System.out.println("Partitions After 1st MapToPair  : " + rddPair1.getNumPartitions());

    JavaPairRDD<String, Iterable<String>> groupRdd = rddPair1.groupByKey();
    System.out.println("Partitions After GroupBy   : " + groupRdd.getNumPartitions());


    /*
    performance optimization to prevent spark from recomputing all RDD's after shuffle
     */
//    groupRdd = groupRdd.cache();
    groupRdd = groupRdd.persist(StorageLevel.MEMORY_AND_DISK());

    groupRdd.foreach(t -> System.out.println("Key: " + t._1() + "---" + "Size: " + Iterables.size(t._2())));



    /*
    Intermediate RDD's calculated are not stored in memory
    When count() is executed, the job looks at the previous shuffle [as data is written to disc when shuffled] ,
    loads data from disc and resumes operation from there.

     */
    System.out.println("Total count:" + groupRdd.count());

    Thread.sleep(100000000);
    sc.close();
  }
}
