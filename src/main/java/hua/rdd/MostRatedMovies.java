package hua.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Collections;

public class MostRatedMovies {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: MostRatedMovies <input-path> <output-path>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("MostRatedMovies");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        JavaRDD<String> movies = spark.textFile(args[0]);
        JavaRDD<String> ratings = spark.textFile(args[1]);

        //movieId from rating
        JavaRDD<String> movieIdFromRating = ratings.flatMap(line -> Collections.singletonList(line.split("::")[1]).iterator());

        //movieId, 1
        JavaPairRDD<String, Integer> moviesPairedWithOne = movieIdFromRating.mapToPair(movieId -> new Tuple2<>(movieId, 1));

        //movieId, total sum of movieId in the dataset
        JavaPairRDD<String, Integer> sumOfEveryMovie = moviesPairedWithOne.reduceByKey(Integer::sum);

        //swap --> total sum of movieId in the dataset, movieId
        JavaPairRDD<Integer, String> swapSumOfMovie = sumOfEveryMovie.mapToPair(swap -> new Tuple2<>(swap._2(),
                swap._1()));

        //Descending order to take the most rated
        JavaPairRDD<Integer, String> sortedSumOfMovie = swapSumOfMovie.sortByKey(false);

        //(MovieId, Count)
        JavaPairRDD<String, Integer> sortedMoviesToCountMostRated = sortedSumOfMovie.mapToPair(movie -> new Tuple2<>(movie._2(), movie._1()));

        //take --> takes the first num of elements in RDD, where DESC order gives the top 25 results
        JavaPairRDD<String, Integer> mostRatedMovies = spark.parallelizePairs(sortedMoviesToCountMostRated.take(25));

        // movieId, movieTitle --> movieId as string in order to join with mostRatedMovies
        JavaPairRDD<String, String> movieTitle = movies.mapToPair(line -> {
            return new Tuple2<>(line.split("::")[0], line.split("::")[1]);
        });

        //movieId, <totalCount of rating, movieTitle>
        JavaPairRDD<String, Tuple2<Integer, String>> moviesJoinedRatings = mostRatedMovies.join(movieTitle);

        //custom pair with total rated counts and movieTitle
        //DESC order in key
        JavaPairRDD<String, String> customMostRatedMovies = moviesJoinedRatings.mapToPair(joined -> {
            return new Tuple2<>("times rated: " + joined._2._1, " movieTitle: " + joined._2._2);
        }).sortByKey(false);

        customMostRatedMovies.saveAsTextFile(args[2]);

        spark.stop();
    }

}
