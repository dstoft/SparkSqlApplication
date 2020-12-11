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

        Function<Row, Boolean> isRetweetFilter = k -> (!k.isNullAt(k.fieldIndex("retweeted_status")));
        Function<Row, Boolean> isTruncatedFilter = k -> (k.getBoolean(k.fieldIndex("truncated")));

        Function<Row, MappedTweet> mapToMappedTweet = row -> {
            long id = row.getLong(row.fieldIndex("id"));
            boolean isRetweet = !row.isNullAt(row.fieldIndex("retweeted_status"));
            boolean isTruncated = row.getBoolean(row.fieldIndex("truncated"));
            String text = row.getString(row.fieldIndex("text"));
            if (isTruncated) {
                Row extendedTweetRow = row.getAs(row.fieldIndex("extended_tweet"));
                text = extendedTweetRow.getString(extendedTweetRow.fieldIndex("full_text"));
            }
            if (isRetweet) {
                Row reweetRow = row.getAs(row.fieldIndex("retweeted_status"));
                text = removeRetweetFromOriginalTweet(text, reweetRow);
            }
            text = text.replace(lineSeparator, " ");

            String timestampMs = row.getString(row.fieldIndex("timestamp_ms"));
            ArrayList<String> words = new ArrayList<>(Arrays.asList(text.split(" ")));
            return new MappedTweet(id, text, timestampMs, words);
        };

        JavaRDD<MappedTweet> rdd = rows
//                .limit(100)
                .filter(rows.col("lang").like("en"))
                .javaRDD()
//                .filter(isTruncatedFilter)
//                .filter(isRetweetFilter)
                .map(mapToMappedTweet);

        rdd.saveAsTextFile(currentDirectory + currentFileName);
    }

    private static String removeRetweetFromOriginalTweet(String originalText, Row retweetRow) {
        String retweetText = retweetRow.getString(retweetRow.fieldIndex("text"));
        int retweetTextIndex = retweetText.indexOf("\\u2026");
        retweetText = retweetTextIndex == -1 ? retweetText.substring(0, retweetText.length() / 2) : retweetText.substring(0, retweetTextIndex);
        int retTweetIndex = originalText.indexOf(retweetText);
        return retTweetIndex == -1 ? "RETWEET!!! REMOVAL!!! ERROR!!!" : originalText.substring(0, retTweetIndex);
    }
}
