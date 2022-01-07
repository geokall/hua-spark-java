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
import java.util.TimeZone;

public class GoodComedyMovies {

    private static final String COMEDY_GENRE = "Comedy";

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

        JavaPairRDD<Integer, MovieDTO> comedyMovies = movieDTOJavaPairRDD.filter(x -> x._2.getGenres().contains(COMEDY_GENRE));

        JavaPairRDD<Integer, RatingDTO> goodRatings = ratingDTOJavaPairRDD.filter(x -> x._2.getRating() >= 3);

        JavaPairRDD<Integer, Tuple2<MovieDTO, RatingDTO>> join = comedyMovies.join(goodRatings);

        JavaRDD<String> goodComedyMovies = join.map(x -> x._2._1.getTitle()).distinct();

        goodComedyMovies.saveAsTextFile(outputPath);

        spark.stop();
    }
}
