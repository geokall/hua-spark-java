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
            System.err.println("Usage: TopRomanticMovies <input-path> <input-path> <output-path>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("TopRomanticMovies");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        JavaRDD<String> movies = spark.textFile(args[0]);
        JavaRDD<String> ratings = spark.textFile(args[1]);

        JavaRDD<String> romanceMovies = movies.filter(x -> {
            String[] split = x.split("::");
            return split[2].contains(ROMANCE_GENRE);
        });

        JavaPairRDD<Integer, String> movieIdAndTitle = romanceMovies.mapToPair(line -> {
            String[] split = line.split("::");
            return new Tuple2<>(Integer.parseInt(split[0]), split[1]);
        });

        JavaRDD<String> decemberRatings = ratings.filter(x -> {
            String[] split = x.split("::");

            LocalDateTime timeStampAsLDT = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(split[3])),
                    TimeZone.getDefault().toZoneId());

            return timeStampAsLDT.getMonth().getValue() == 12;
        });

        JavaPairRDD<Integer, Double> ratingsMovieIdRating = decemberRatings.mapToPair(line -> {
            String[] split = line.split("::");

            return new Tuple2<>(Integer.parseInt(split[1]), Double.parseDouble(split[2]));
        });

        JavaPairRDD<Integer, Tuple2<Double, String>> join = ratingsMovieIdRating.join(movieIdAndTitle);

        JavaPairRDD<Integer, Iterable<Tuple2<Double, String>>> grouped = join.groupByKey();

        JavaPairRDD<Integer, Double> pairOfMovieIdAndSumRating = grouped.mapToPair(group -> {
            double sumRating = 0;

            for (Tuple2<Double, String> doubleStringTuple2 : group._2) {
                sumRating = sumRating + doubleStringTuple2._1;
            }

            return new Tuple2<>(group._1, sumRating);
        });

        JavaPairRDD<Integer, Tuple2<Double, String>> joined = pairOfMovieIdAndSumRating.join(movieIdAndTitle);

        JavaPairRDD<Double, String> sortedSumRating = joined.
                mapToPair(both -> new Tuple2<>(both._2._1, both._2._2))
                .sortByKey(false);

        JavaPairRDD<Double, String> topRomanticMoviesBasedOnDecemberRating = spark.parallelizePairs(sortedSumRating.take(10));

        topRomanticMoviesBasedOnDecemberRating.saveAsTextFile(args[2]);

        spark.stop();
    }
}
