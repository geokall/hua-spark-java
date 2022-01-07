package hua.rdd;

import hua.dto.MovieDTO;
import hua.dto.RatingDTO;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;


public class TopRomanticMovies {

    private static final String ROMANCE_GENRE = "Romance";

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

        JavaPairRDD<Integer, MovieDTO> movieDTOJavaPairRDD = moviesTextFile.mapToPair((PairFunction<String, Integer, MovieDTO>) s -> {
            String[] split = s.split("::");

            MovieDTO movieDTO = new MovieDTO();
            movieDTO.setMovieId(Integer.parseInt(split[0]));
            movieDTO.setTitle(split[1]);
            movieDTO.setGenres(split[2]);

            return new Tuple2<>(Integer.parseInt(split[0]), movieDTO);
        });

        JavaPairRDD<Integer, RatingDTO> ratingDTOJavaPairRDD = ratingsTextFile.mapToPair((PairFunction<String, Integer, RatingDTO>) s -> {
            String[] split = s.split("::");

            RatingDTO ratingDTO = new RatingDTO();
            ratingDTO.setUserId(Integer.parseInt(split[0]));
            ratingDTO.setMovieId(Integer.parseInt(split[1]));
            ratingDTO.setRating(Double.parseDouble(split[2]));

            //long timeStamp to LocalDateTime in order to get December Month
            LocalDateTime timeStampAsLDT = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(split[3])),
                    TimeZone.getDefault().toZoneId());

            ratingDTO.setTimeStampParsed(timeStampAsLDT);

            return new Tuple2<>(Integer.parseInt(split[1]), ratingDTO);
        });

        //Romance movies
        JavaPairRDD<Integer, MovieDTO> romanceMovies = movieDTOJavaPairRDD.filter(moviePair -> {
            return moviePair._2.getGenres().contains(ROMANCE_GENRE);
        });

        //December ratings
        JavaPairRDD<Integer, RatingDTO> decemberRatings = ratingDTOJavaPairRDD.filter(ratingPair -> {
            return ratingPair._2.getTimeStampParsed().getMonth().getValue() == 12;
        });

        //movieId, join
        JavaPairRDD<Integer, Tuple2<RatingDTO, MovieDTO>> join = decemberRatings.join(romanceMovies);

        //grouped by movieId on rating dataset
        JavaPairRDD<Integer, Iterable<Tuple2<RatingDTO, MovieDTO>>> grouped = join.groupByKey();

        //movieId, sum rating
        JavaPairRDD<Integer, Double> pairOfMovieIdAndSumRating = grouped.mapToPair(group -> {
            double sumRating = 0;

            for (Tuple2<RatingDTO, MovieDTO> ratingDTOMovieDTOTuple2 : group._2) {
                sumRating = sumRating + ratingDTOMovieDTOTuple2._1.getRating();
            }

            return new Tuple2<>(group._1, sumRating);
        });

        //to retrieve title information
        JavaPairRDD<Integer, Tuple2<Double, MovieDTO>> joined = pairOfMovieIdAndSumRating.join(romanceMovies);

        //title, sumRating
        JavaPairRDD<String, Double> titleSumRating = joined.mapToPair(both -> {
            return new Tuple2<>(both._2._2.getTitle(), both._2._1);
        });

        //sumRating, title
        JavaPairRDD<Double, String> toSort = titleSumRating.mapToPair(Tuple2::swap);

        //DESC order on sumRating
        JavaPairRDD<Double, String> doubleStringJavaPairRDD = toSort.sortByKey(false);

        JavaPairRDD<Double, String> topRomanticMoviesBasedOnDecemberRating = spark.parallelizePairs(doubleStringJavaPairRDD.take(10));

        topRomanticMoviesBasedOnDecemberRating.saveAsTextFile(outputPath);

        spark.stop();
    }
}
