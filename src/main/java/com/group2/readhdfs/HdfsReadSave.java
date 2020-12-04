package com.group2.readhdfs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HdfsReadSave {
    public static void main(String[] args) {
        //System.out.println("Printing wow!!");
        SparkSession spark = SparkSession.builder().appName("HdfsReadSave").getOrCreate();

        Dataset<Row> rows = spark.read().json("hdfs://10.123.252.244:9000/user/hadoop/twitter-files/coronavirus_tweets_20200127.txt");

        // live-twitter-files/FlumeData.1607083399947
//        Dataset<Row> rows = spark.read().format("org.apache.spark.sql.avro").load("hdfs://10.123.252.244:9000/user/hadoop/live-twitter-files/FlumeData.1607087977897.avro");

        rows.select("created_at","id","text").limit(10).write().format("csv").save("hdfs://10.123.252.244:9000/user/hadoop/twitter-java-save-text");

        //spark.readStream().format("parquet").option("inferschema","true").option("maxFilesPerTrigger", "1").option("header", "true").load()
    }
}
