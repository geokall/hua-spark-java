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
            System.err.println("Usage: GoodComedyMovies <input-path> <input-path> <output-path>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("GoodComedyMovies");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        JavaRDD<String> movies = spark.textFile(args[0]);
        JavaRDD<String> ratings = spark.textFile(args[1]);

        JavaPairRDD<Integer, String> movieIdAndComedyGenre = movies
                .mapToPair(GoodComedyMovies::toMovieIdAndGenre)
                .filter(x -> x._2.contains(COMEDY_GENRE));

        JavaPairRDD<Integer, Double> movieIdAndGoodRating = ratings
                .mapToPair(GoodComedyMovies::toMovieIdAndRating)
                .filter(x -> x._2 >= 3);

        JavaPairRDD<Integer, Tuple2<String, Double>> join = movieIdAndComedyGenre.join(movieIdAndGoodRating);

        JavaRDD<Integer> distinctMovieIds = join.map(x -> x._1).distinct();

        long count = distinctMovieIds.count();

        JavaPairRDD<String, Long> tupleOfTotalComedyMovies = distinctMovieIds
                .mapToPair(x -> new Tuple2<>("totalComedyMovies", count));

        JavaPairRDD<String, Long> goodComedyMovies = spark.parallelizePairs(tupleOfTotalComedyMovies.take(1));

        goodComedyMovies.saveAsTextFile(args[2]);

        spark.stop();
    }


    private static Tuple2<Integer, Double> toMovieIdAndRating(String line) {
        String[] split = line.split("::");
        return new Tuple2<>(Integer.parseInt(split[1]), Double.parseDouble(split[2]));
    }

    private static Tuple2<Integer, String> toMovieIdAndGenre(String line) {
        String[] split = line.split("::");
        return new Tuple2<>(Integer.parseInt(split[0]), split[2]);
    }
}
