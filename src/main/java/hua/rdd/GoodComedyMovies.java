package hua.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class GoodComedyMovies {

    private static final String COMEDY_GENRE = "Comedy";

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: GoodComedyMovies <input-path> <output-path>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("GoodComedyMovies");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        JavaRDD<String> movies = spark.textFile(args[0]);
        JavaRDD<String> ratings = spark.textFile(args[1]);

        //movieId, genre
        JavaPairRDD<Integer, String> movieIdAndGenres = movies.mapToPair(line -> {
            String[] split = line.split("::");
            return new Tuple2<>(Integer.parseInt(split[0]), split[2]);
        });

        //movieId, comedy genre
        JavaPairRDD<Integer, String> movieIdAndComedyGenre = movieIdAndGenres.filter(x -> x._2.contains(COMEDY_GENRE));

        //movieId(duplicate), rating
        JavaPairRDD<Integer, Double> movieIdAndRating = ratings.mapToPair(line -> {
            String[] split = line.split("::");
            return new Tuple2<>(Integer.parseInt(split[1]), Double.parseDouble(split[2]));
        });

        //movieId(duplicate), goodRating
        JavaPairRDD<Integer, Double> movieIdAndGoodRating = movieIdAndRating.filter(x -> x._2 >= 3);

        //movieId, <genre, rating>
        JavaPairRDD<Integer, Tuple2<String, Double>> joinBabe = movieIdAndComedyGenre.join(movieIdAndGoodRating);

        JavaRDD<Integer> distinctMovieIds = joinBabe.map(x -> x._1).distinct();

        long count = distinctMovieIds.count();
        int sum = (int) count;

        JavaPairRDD<String, Integer> lalakis = distinctMovieIds.mapToPair(x -> {
            return new Tuple2<>("totalComedyMovies", sum);
        });

        //tuple return list, so taking the first
        JavaPairRDD<String, Integer> goodComedyMovies = spark.parallelizePairs(lalakis.take(1));

        goodComedyMovies.saveAsTextFile(args[2]);

        spark.stop();
    }
}
