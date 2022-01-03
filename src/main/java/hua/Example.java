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
//import static org.apache.spark.sql.functions.*;


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

        SparkSession spark;
        String inputPath, outputPath;
        if (isLocal) {
            spark = SparkSession.builder().master("local").appName("Java Spark SQL example")
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
        Dataset<String> movies = spark.read().textFile(inputPath + "/movies.dat");
        Dataset<String> ratings = spark.read().textFile(inputPath + "/ratings.dat");

        movies.printSchema();
        movies.show();

        movies.join(ratings);


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
