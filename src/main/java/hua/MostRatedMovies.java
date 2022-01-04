package hua;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class MostRatedMovies {

    public static void main(String[] args) throws Exception {

        boolean isLocal = false;

        if (args.length == 0) {
            isLocal = true;
        } else if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        String inputPath = "src/main/resources";
        String outputPath = "output";

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Example");
        sparkConf.setMaster("local[4]");
        sparkConf.set("spark.driver.bindAddress", "127.0.0.1");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        FileUtils.deleteDirectory(new File("output"));

        JavaRDD<String> moviesTextFile = spark.textFile(inputPath + "/movies.dat");
        JavaRDD<String> ratingsTextFile = spark.textFile(inputPath + "/ratings.dat");

        //movieId from rating
        JavaRDD<String> movieIdFromRating = ratingsTextFile.flatMap(line -> Arrays.asList(line.split("::")[1]).iterator());

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
        JavaPairRDD<String, String> movieTitle = moviesTextFile.mapToPair(line -> {
            return new Tuple2<>(line.split("::")[0], line.split("::")[1]);
        });

        //movieId, <totalCount of rating, movieTitle>
        JavaPairRDD<String, Tuple2<Integer, String>> moviesJoinedRatings = mostRatedMovies.join(movieTitle);

        System.out.println("----- Top 25 rated movies by users -----");
        System.out.println(moviesJoinedRatings.take(25));

        //custom pair with total rated counts and movieTitle
        //DESC order in key
        JavaPairRDD<String, String> integerStringJavaPairRDD = moviesJoinedRatings.mapToPair(joined -> {
            return new Tuple2<>("times rated: " + joined._2._1, " movieTitle: " + joined._2._2);
        }).sortByKey(false);

//        for (Tuple2<String, Tuple2<Integer, String>> wordToCount : moviesJoinedRatings.collect()) {
//            System.out.println(wordToCount._1() + " : " + wordToCount._2());
//        }

        integerStringJavaPairRDD.saveAsTextFile(outputPath);

        spark.stop();
    }

}
