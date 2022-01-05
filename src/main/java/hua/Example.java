package hua;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;
import static org.apache.spark.sql.functions.*;


public class Example {

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
            spark = SparkSession.builder().master("local[4]")
                    .appName("Java Spark SQL example")
                    .getOrCreate();
            inputPath = "src/main/resources";
            outputPath = "output";
        } else {
            spark = SparkSession.builder().appName("Java Spark SQL example")
                    .getOrCreate();
            inputPath = args[0];
            outputPath= args[1];
        }

        SparkContext sparkContext = spark.sparkContext();

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

        Dataset<String> stringDataset = spark.read().textFile(inputPath + "/ratings.dat");

        JavaRDD<RatingWithMonthDTO> map = stringDataset.javaRDD().map(line -> {
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
        Dataset<Row> ratings = spark.createDataFrame(map, RatingWithMonthDTO.class);
        Dataset <Row> joined = movies.join(ratings, movies.col("movieId").equalTo(ratings.col("movieId")));

//        SELECT title
//        FROM movie m
//        JOIN (
//                SELECT movieid, COUNT(movieid)
//                FROM ratings GROUP BY movieid)
//        ON m.movieid = r.movieid
//        LIMIT 10
        Dataset<Row> agg = movies.join(ratings, movies.col("movieId").equalTo(ratings.col("movieId")))
                .groupBy(ratings.col("movieId"))
                .agg(count(ratings.col("rating")))
                .orderBy(org.apache.spark.sql.functions.col("count(rating)").desc());
        Dataset<Row> limit = agg.limit(10);
        limit.show();


        // get all comedies
//        Dataset<Row> allComedies = movies.filter(movies.col("genres").like("%Comedy%"));
//        allComedies.show();
//        allComedies.write().format("json").save(outputPath+"/all-comedies");
//
//        // TODO: count all comedies that a user rates at least 3.0
//        //       (join ratings with movies, filter by rating, groupby userid and
//        //        aggregate count)
//        Dataset<Row> goodComediesPerUser = /*???*/null/*???*/;
//
//        goodComediesPerUser.show();
//        goodComediesPerUser.write().format("json").save(outputPath+"/good-comedies-per-user");


        spark.close();

    }
}
