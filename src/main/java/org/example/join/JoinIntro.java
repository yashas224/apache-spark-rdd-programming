package org.example.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JoinIntro {
  public static void main(String[] args) {

    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkConf conf = new SparkConf().setAppName("Join-spark-intro").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Tuple2<Integer, Integer>> viewList = new ArrayList<>();
    viewList.add(new Tuple2<>(1, 10));
    viewList.add(new Tuple2<>(2, 15));
    viewList.add(new Tuple2<>(3, 100));
    viewList.add(new Tuple2<>(8, 1));
    viewList.add(new Tuple2<>(10, 2));

    List<Tuple2<Integer, String>> userList = new ArrayList<>();
    userList.add(new Tuple2<>(1, "ADAM"));
    userList.add(new Tuple2<>(2, "EVE"));
    userList.add(new Tuple2<>(3, "Mark"));
    userList.add(new Tuple2<>(4, "david"));
    userList.add(new Tuple2<>(5, "test Name"));

    JavaPairRDD<Integer, Integer> viewPairRDD = sc.parallelizePairs(viewList);
    JavaPairRDD<Integer, String> userPairRDD = sc.parallelizePairs(userList);

    System.out.println("-------INNER JOIN----------");

    viewPairRDD
       .join(userPairRDD)
       .foreach(pair -> System.out.println(pair));

    System.out.println("-------LEFT JOIN----------");
    viewPairRDD
       .leftOuterJoin(userPairRDD)
       .foreach(pair -> System.out.println(pair));

    System.out.println("-------RIGHT JOIN----------");
    viewPairRDD
       .rightOuterJoin(userPairRDD)
       .foreach(pair -> System.out.println(pair));

    System.out.println("-------cartesian product----------");

    viewPairRDD
       .cartesian(userPairRDD)
       .foreach(pair -> System.out.println(pair));
  }
}
