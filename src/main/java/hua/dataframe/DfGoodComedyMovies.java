package hua.dataframe;

import hua.dto.MovieDTO;
import hua.dto.RatingDTO;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class DfGoodComedyMovies {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println("Usage: DfGoodComedyMovies input-path input-path output-path");
            System.exit(0);
        }

        SparkSession spark = SparkSession.builder().appName("DfGoodComedyMovies")
                .getOrCreate();

        String movies = args[0];
        String ratings = args[1];

        String outputPath = args[2];

        JavaRDD<MovieDTO> moviesRDD = spark.read()
                .textFile(movies)
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
                .textFile(ratings)
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("::");

                    RatingDTO ratingDTO = new RatingDTO();
                    ratingDTO.setUserId(Integer.parseInt(parts[0]));
                    ratingDTO.setMovieId(Integer.parseInt(parts[1]));
                    ratingDTO.setRating(Double.parseDouble(parts[2]));

                    return ratingDTO;
                });

        Dataset<Row> moviesDataset = spark.createDataFrame(moviesRDD, MovieDTO.class);
        Dataset<Row> ratingsDataset = spark.createDataFrame(ratingsRDD, RatingDTO.class);

        Dataset<Row> comedyMovies = moviesDataset.filter(moviesDataset.col("genres").like("%Comedy%"));
        Dataset<Row> goodRatings = ratingsDataset.filter(ratingsDataset.col("rating").geq(3));

        Dataset<Row> goodComedyMovies = comedyMovies
                .join(goodRatings, comedyMovies.col("movieId").equalTo(goodRatings.col("movieId")))
                .select(countDistinct("title"));

        Dataset<Row> totalGoodComedyMovies = goodComedyMovies.withColumnRenamed("count(DISTINCT title)", "totalGoodComedyMovies");

        totalGoodComedyMovies.write().format("json").save(outputPath);

        spark.close();
    }
}
