package hua.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.*;
import java.util.*;

public class TopRomanticMovies {

    private static final String ROMANCE_GENRE = "Romance";

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: TopRomanticMovies <input-path> <output-path>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("TopRomanticMovies");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        JavaRDD<String> movies = spark.textFile(args[0]);
        JavaRDD<String> ratings = spark.textFile(args[1]);

        //romance
        JavaRDD<String> romanceMovies = movies.filter(x -> {
            String[] split = x.split("::");
            return split[2].contains(ROMANCE_GENRE);
        });

        //movieId, movieTitle
        JavaPairRDD<Integer, String> movieIdAndTitle = romanceMovies.mapToPair(line -> {
            String[] split = line.split("::");
            return new Tuple2<>(Integer.parseInt(split[0]), split[1]);
        });

        JavaRDD<String> decemberRatings = ratings.filter(x -> {
            String[] split = x.split("::");

            //running on HDFS default TimeZone is UTC
            LocalDateTime timeStampAsLDT = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(split[3])),
                    TimeZone.getDefault().toZoneId());

            return timeStampAsLDT.getMonth().getValue() == 12;
        });

        //movieId(duplicate), rating
        JavaPairRDD<Integer, Double> ratingsMovieIdRating = decemberRatings.mapToPair(line -> {
            String[] split = line.split("::");

            return new Tuple2<>(Integer.parseInt(split[1]), Double.parseDouble(split[2]));
        });

        JavaPairRDD<Integer, Tuple2<Double, String>> join = ratingsMovieIdRating.join(movieIdAndTitle);

        //grouped by movieId on rating dataset
        JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> grouped = join.groupByKey();

        //movieId, sum rating
        JavaPairRDD<Integer, Double> pairOfMovieIdAndSumRating = grouped.mapToPair(group -> {
            double sumRating = 0;

            for (Tuple2<Double, String> doubleStringTuple2 : group._2) {
                sumRating = sumRating + doubleStringTuple2._1;
            }

            return new Tuple2<>(group._1, sumRating);
        });

        //to retrieve title information
        JavaPairRDD<Integer, Tuple2<Double, String>> joined = pairOfMovieIdAndSumRating.join(movieIdAndTitle);

        //title, sumRating
        JavaPairRDD<String, Double> titleSumRating = joined.mapToPair(both -> new Tuple2<>(both._2._2, both._2._1));

        //sumRating, title
        JavaPairRDD<Double, String> sumRatingTitle = titleSumRating.mapToPair(Tuple2::swap);

        //DESC order on sumRating
        JavaPairRDD<Double, String> sortedSumRating = sumRatingTitle.sortByKey(false);

        JavaPairRDD<Double, String> topRomanticMoviesBasedOnDecemberRating = spark.parallelizePairs(sortedSumRating.take(10));

        topRomanticMoviesBasedOnDecemberRating.saveAsTextFile(args[2]);

        spark.stop();
    }
}
