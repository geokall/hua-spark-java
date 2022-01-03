package hua;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class RDD {

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

        JavaRDD<String> movies = spark.textFile(inputPath + "/movies.dat");
        JavaRDD<String> ratings = spark.textFile(inputPath + "/ratings.dat");

        JavaPairRDD<Integer, MovieDTO> idMovieDTO = movies.mapToPair(new PairFunction<String, Integer, MovieDTO>() {

            @Override
            public Tuple2<Integer, MovieDTO> call(String s) throws Exception {
                String[] split = s.split("::");

                MovieDTO movieDTO = new MovieDTO();
                movieDTO.setMovieId(Integer.parseInt(split[0]));
                movieDTO.setTitle(split[1]);
                movieDTO.setGenres(split[2]);

                return new Tuple2<>(Integer.parseInt(split[0]), movieDTO);
            }
        });

        JavaPairRDD<Integer, RatingDTO> idRatingDTO = ratings.mapToPair(new PairFunction<String, Integer, RatingDTO>() {

            @Override
            public Tuple2<Integer, RatingDTO> call(String s) throws Exception {
                String[] split = s.split("::");

                RatingDTO ratingDTO = new RatingDTO();
                ratingDTO.setUserId(Integer.parseInt(split[0]));
                ratingDTO.setMovieId(Integer.parseInt(split[1]));
                ratingDTO.setRating(Double.parseDouble(split[2]));
                ratingDTO.setTimeStamp(Long.parseLong(split[3]));

                return new Tuple2<>(Integer.parseInt(split[1]), ratingDTO);
            }
        });

//
//        JavaPairRDD<Integer, Tuple2<MovieDTO, RatingDTO>> joined = idMovieDTO.join(idRatingDTO);
//
//        JavaPairRDD<Integer, Iterable<Tuple2<MovieDTO, RatingDTO>>> integerIterableJavaPairRDD1 = joined.groupByKey(); //key is the movieId
//
//        integerIterableJavaPairRDD1.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(joined.getNumPartitions());
//
//        JavaRDD<Integer> map = joined.filter(x -> {
//            return x._2._2.getMovieId().intValue() == x._2._1.getMovieId().intValue();
//        }).map(l -> l._2._1.getMovieId()).distinct();
//
//        map.saveAsTextFile(outputPath);

        ///test
        // System.out.println(getmovieidanduserid());
        JavaRDD<String> movieIdFromRating = ratings.flatMap(line -> Arrays.asList(line.split("::")[1]).iterator());

        JavaPairRDD<String, Integer> moviesPaired = movieIdFromRating.mapToPair(movieId -> new Tuple2<>(movieId, 1));

        JavaPairRDD<String, Integer> sumOfEveryMovie = moviesPaired.reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> swapSumOfMovie = sumOfEveryMovie.mapToPair(wordToCount -> new Tuple2<>(wordToCount._2(),
                wordToCount._1()));

        JavaPairRDD<Integer, String> sortedSumOfMovie = swapSumOfMovie.sortByKey(false);

        // (MovieId, Count)
        JavaPairRDD<String, Integer> sortedMoviesToCountMostRated = sortedSumOfMovie.mapToPair(movie -> new Tuple2<>(movie._2(), movie._1()));

        JavaPairRDD<String, Integer> mostRatedMovies = spark.parallelizePairs(sortedMoviesToCountMostRated.take(25));

        JavaPairRDD<String, String> moviesiduser = movies.mapToPair(getPairFunction());

        JavaPairRDD<String, Tuple2<Integer, String>> joindata = mostRatedMovies.join(moviesiduser);
        System.out.println("-------top movies----------");
        System.out.println(joindata.take(25));

        joindata.saveAsTextFile(outputPath);

        for (Tuple2<String, Tuple2<Integer, String>> wordToCount : joindata.collect()) {
            System.out.println(wordToCount._1() + " : " + wordToCount._2());
        }

//        mostRatedMovies.saveAsTextFile(outputPath);

        System.out.println(mostRatedMovies.take(25));

        spark.stop();

    }

    private static PairFunction<String, String, String> getmovieidanduserid() {
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split("::")[1],
                line.split("::")[0]);
    }

    private static PairFunction<String, String, String> getPairFunction() {
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split("::")[0],
                line.split("::")[1]);
    }

}
