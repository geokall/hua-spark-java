package hua.dataframe;

import hua.dto.MovieDTO;
import hua.dto.RatingWithMonthDTO;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

import static org.apache.spark.sql.functions.*;

public class DfMostRatedOnDecember {

    public static void main(String[] args) throws Exception {

        boolean isLocal = false;

        if (args.length == 0) {
            isLocal = true;
        } else if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        SparkSession spark;
        String inputPath, outputPath;

        if (isLocal) {
            spark = SparkSession.builder().master("local")
                    .appName("Java Spark SQL example")
                    .getOrCreate();
            inputPath = "src/main/resources";
            outputPath = "output";
        } else {
            spark = SparkSession.builder().appName("Java Spark SQL example")
                    .getOrCreate();
            inputPath = args[0];
            outputPath = args[1];
        }

        FileUtils.deleteDirectory(new File("output"));

        JavaRDD<MovieDTO> moviesRDD = spark.read()
                .textFile(inputPath + "/movies.dat")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("::");

                    MovieDTO movieDTO = new MovieDTO();
                    movieDTO.setMovieId(Integer.parseInt(parts[0]));
                    movieDTO.setTitle(parts[1]);
                    movieDTO.setGenres(parts[2]);

                    return movieDTO;
                });

        JavaRDD<RatingWithMonthDTO> ratingsRDD = spark.read()
                .textFile(inputPath + "/ratings.dat")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("::");

                    RatingWithMonthDTO ratingDTO = new RatingWithMonthDTO();
                    ratingDTO.setUserId(Integer.parseInt(parts[0]));
                    ratingDTO.setMovieId(Integer.parseInt(parts[1]));
                    ratingDTO.setRating(Double.parseDouble(parts[2]));

                    LocalDateTime timeStampAsLDT = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(parts[3])),
                            TimeZone.getDefault().toZoneId());

                    int month = timeStampAsLDT.getMonth().getValue();

                    ratingDTO.setMonth(month);

                    return ratingDTO;
                });

        Dataset<Row> movies = spark.createDataFrame(moviesRDD, MovieDTO.class);
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, RatingWithMonthDTO.class);

        Dataset<Row> decemberRatings = ratings.filter(ratings.col("month").equalTo(12));

        Dataset<Row> mostRatedOnDecember = movies.join(decemberRatings, movies.col("movieId").equalTo(decemberRatings.col("movieId")))
                .groupBy(decemberRatings.col("movieId"), movies.col("title"))
                .agg(count(decemberRatings.col("rating")))
                .orderBy(col("count(rating)").desc())
                .select("title");

        mostRatedOnDecember.write().format("json").save(outputPath);

        spark.close();
    }
}
