package hua.dataframe;

import hua.dto.MovieDTO;
import hua.dto.RatingDTO;
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

public class DfTopRomanticMovies {

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

        //creating column names based on split
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

        JavaRDD<RatingDTO> ratingsRDD = spark.read()
                .textFile(inputPath + "/ratings.dat")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("::");

                    RatingDTO ratingDTO = new RatingDTO();
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
        Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, RatingDTO.class);

        Dataset<Row> romanceMovies = movies.filter(movies.col("genres").like("%Romance%"));
        Dataset<Row> decemberRatings = ratings.filter(ratings.col("month").equalTo(12));

        System.out.println("c1:" + romanceMovies.count());
        System.out.println("c2:" + decemberRatings.count());

        Dataset<Row> topRomanticMoviesBasedOnDecemberRating = romanceMovies
                .join(decemberRatings, romanceMovies.col("movieId").equalTo(decemberRatings.col("movieId")))
                .groupBy(decemberRatings.col("movieId"), romanceMovies.col("title"))
                .agg(sum("rating"))
                .orderBy(col("sum(rating)").desc())
                .select("title", "sum(rating)")
                .limit(10);

        topRomanticMoviesBasedOnDecemberRating.write().format("json").save(outputPath);

        spark.close();
    }
}
