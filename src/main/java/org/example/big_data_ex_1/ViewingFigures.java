package org.example.big_data_ex_1;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

/*

FInd the sore of the course based on logic by looking at the users view data

 */
public class ViewingFigures {
  public static void main(String[] args) throws InterruptedException {
    Logger.getLogger("org.apache").setLevel(Level.INFO);

    SparkConf conf = new SparkConf().setAppName("big-data-1").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

//    (userId,chapterId)
    JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc);

//    (chapterId,courseId)
    JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc);

//    (courseId, title)
    JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc);

    // distinct (userId chapterID)
    viewData = viewData.distinct();

    // (course ID , Ttaal NosOfChapters)
    JavaPairRDD<Integer, Integer> courseIdToTotalChapters = chapterData
       .mapToPair(t -> new Tuple2<>(t._2(), 1))
       .reduceByKey(Integer::sum);

    //  (chapterId, userId)
    JavaPairRDD<Integer, Integer> chapterIduserId = viewData.mapToPair(t -> new Tuple2<>(t._2(), t._1()));

    //  (chapter ID, (user ID, CourseId))
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRDD = chapterIduserId.join(chapterData);

    // (user ID, course ID) , 1)
    // each RDD represents chapter user read from course
    JavaPairRDD<Tuple2<Integer, Integer>, Long> rdd = joinedRDD.mapToPair(t -> {
      Tuple2<Integer, Integer> key = new Tuple2<>(t._2()._1(), t._2()._2());
      return new Tuple2<>(key, 1L);
    });

    // ((user ID, course ID) , total chapters taken by users per course)

    rdd = rdd.reduceByKey(Long::sum);

    //
//    (course ID ,total chapters taken per users )
    JavaPairRDD<Integer, Long> courseIdToChapters = rdd.mapToPair(t -> new Tuple2<>(t._1()._2(), t._2()));

    //(courseID,  (total chapters taken per users, total Chapters in the course ))
    // inner join

    JavaPairRDD<Integer, Tuple2<Long, Integer>> rdd2 = courseIdToChapters.join(courseIdToTotalChapters);

//     (courseID, % values for each course per user)
    JavaPairRDD<Integer, Double> courseIdToPercentage = rdd2
       .mapValues(value -> {
         int watchedChaptersPerUserPerCourse = Math.toIntExact(value._1());
         int totalChaptersInCourse = value._2();
         return (watchedChaptersPerUserPerCourse * 1.0 / totalChaptersInCourse) * 100;
       });

    // (final course Id , score)
    JavaPairRDD<Integer, Integer> rdd3 = courseIdToPercentage.mapValues(v1 -> {
      if(v1 > 90) return 10;
      else if(v1 > 50) return 4;
      else if(v1 > 25) return 2;
      else return 0;
    });

    // (final course Id , total score)
    JavaPairRDD<Integer, Integer> finalRDD = rdd3.reduceByKey(Integer::sum);

//    (final course Id , (total score, Course title )
    JavaPairRDD<Integer, Tuple2<Integer, String>> joined = finalRDD.join(titlesData);

    JavaPairRDD<Integer, String> rdd7 = joined.mapToPair(t -> new Tuple2<>(t._2()._1(), t._2()._2()));

    List resultList = rdd7.sortByKey(false).collect();

    resultList.forEach(t -> System.out.println(t));

    Thread.sleep(1000000);
    sc.close();
  }

  private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc) {

    return sc.textFile("src/main/resources/viewing figures/titles.csv")
       .mapToPair(commaSeparatedLine -> {
         String[] cols = commaSeparatedLine.split(",");
         return new Tuple2<Integer, String>(new Integer(cols[0]), cols[1]);
       });
  }

  private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc) {

    return sc.textFile("src/main/resources/viewing figures/chapters.csv")
       .mapToPair(commaSeparatedLine -> {
         String[] cols = commaSeparatedLine.split(",");
         return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
       }).cache();
  }

  private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc) {

    return sc.textFile("src/main/resources/viewing figures/views-*.csv")
       .mapToPair(commaSeparatedLine -> {
         String[] columns = commaSeparatedLine.split(",");
         return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
       });
  }
}
