package com.group2.readhdfs;

import com.group2.readhdfs.models.MappedTweet;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;

public class HdfsReadSave {
    public static void main(String[] args) {
        String currentDirectory = "hdfs://10.123.252.244:9000/user/hadoop/twitter-java-save-txt/";
        String currentFileName = "V01Sql." + System.currentTimeMillis();
        String lineSeparator = System.getProperty("line.separator");

        //System.out.println("Printing wow!!");
        SparkSession spark = SparkSession.builder().appName("HdfsReadSave").getOrCreate();

        Dataset<Row> rows = spark.read().json("hdfs://10.123.252.244:9000/user/hadoop/twitter-files/coronavirus_tweets_20200128_2.txt");

        JavaRDD<MappedTweet> rdd = rows.limit(100).filter(rows.col("lang").like("en")).javaRDD().map((Function<Row, MappedTweet>) row -> {
            long id = row.getLong(row.fieldIndex("id"));
            String text = row.getString(row.fieldIndex("text")).replace(lineSeparator, " ");
            String timestampMs = row.getString(row.fieldIndex("timestamp_ms"));
            ArrayList<String> words = new ArrayList<>(Arrays.asList(text.split(" ")));
            return new MappedTweet(id, text, timestampMs, words);
        });

        rdd.saveAsTextFile(currentDirectory + currentFileName);
    }
}
