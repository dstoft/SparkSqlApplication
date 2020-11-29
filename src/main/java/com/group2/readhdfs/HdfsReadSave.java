package com.group2.readhdfs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HdfsReadSave {
    public static void main(String[] args) {
        //System.out.println("Printing wow!!");
        SparkSession spark = SparkSession.builder().appName("HdfsReadSave").getOrCreate();

        Dataset<Row> rows = spark.read().json("hdfs://10.123.252.244:9000/user/hadoop/twitter-files/coronavirus_tweets_20200127.txt");
        rows.select("created_at","id","text").limit(10).write().save("hdfs://10.123.252.244:9000/user/hadoop/twitter-java-save-test");
    }
}
