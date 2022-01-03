package hua;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

public class Example {

    public static void main(String[] args) throws Exception {

        boolean isLocal = false;
        if (args.length == 0) {
            isLocal = true;
        } else if (args.length < 2) {
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

//        SparkSession spark;
//        String inputPath, outputPath;
//        if (isLocal) {
//            spark = SparkSession.builder().master("local").appName("Java Spark SQL example")
//                    .getOrCreate();
//            inputPath = "src/main/resources";
//            outputPath = "output";
//        } else {
//            spark = SparkSession.builder().appName("Java Spark SQL example")
//                    .getOrCreate();
//            inputPath = args[0];
//            outputPath= args[1];
//        }
//
//        SparkContext sparkContext = spark.sparkContext();
//        Dataset<String> stringDataset = spark.read().textFile(inputPath + "/movies.dat");

        String inputPath = "src/main/resources";
        String outputPath = "output";

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Example");
        sparkConf.setMaster("local[4]");
        sparkConf.set("spark.driver.bindAddress", "127.0.0.1");

        JavaSparkContext spark = new JavaSparkContext(sparkConf);

        JavaRDD<String> movies = spark.textFile(inputPath + "/movies.dat");
        JavaRDD<String> ratings = spark.textFile(inputPath + "/ratings.dat");

        JavaRDD<MovieDTO> splitMovies = movies.map(movie -> {
            String[] split = movie.split("::");

            return new MovieDTO(Integer.parseInt(split[0]), split[1], Collections.singletonList(split[2]));
        });

        JavaRDD<RatingDTO> splitRatings = ratings.map(rating -> {
            String[] split = rating.split("::");

            return new RatingDTO(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Double.parseDouble(split[2]), Long.parseLong(split[3]));
        });

        splitMovies.foreach(e -> {
            System.out.println(e.getTitle());
        });
//        System.out.println(movies);


        // load
//        Dataset<Row> movies = spark.read().option("header", "true").csv(inputPath+"/movies.csv");
//        Dataset<Row> links = spark.read().option("header", "true").csv(inputPath+"/links.csv");
//        Dataset<Row> ratings = spark.read().option("header", "true").csv(inputPath+"/ratings.csv");
//        Dataset<Row> tags = spark.read().option("header", "true").csv(inputPath+"/tags.csv");

        // schema
        //
        // ratings.csv   userId,movieId,rating,timestamp
        // movies.csv    movieId,title,genres
        // links.csv     movieId,imdbId,tmdbId
        // tags.csv      userId,movieId,tag,timestamp
        // print schema
//        movies.printSchema();
//        links.printSchema();
//        ratings.printSchema();
//        tags.printSchema();
//
//        // print some data
//        movies.show();
//        links.show();
//        ratings.show();
//        tags.show();

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
