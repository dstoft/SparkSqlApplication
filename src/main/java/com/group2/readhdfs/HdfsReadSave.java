package com.group2.readhdfs;

import com.group2.readhdfs.models.MappedTweet;
import com.group2.readhdfs.models.SentimentLists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HdfsReadSave {
    public static void main(String[] args) {
        String currentDirectory = "hdfs://10.123.252.244:9000/user/hadoop/twitter-java-save-txt/";
        String currentFileName = "V01Sql." + System.currentTimeMillis();
        String lineSeparator = System.getProperty("line.separator");

        //System.out.println("Printing wow!!");
        SparkSession spark = SparkSession.builder().appName("HdfsReadSave").getOrCreate();

        Dataset<Row> rows = spark.read().json("hdfs://10.123.252.244:9000/user/hadoop/twitter-files/coronavirus_tweets_20200127.txt");

        Function<Row, Boolean> isNotRetweetFilter = k -> (k.isNullAt(k.fieldIndex("retweeted_status")));
        Function<Row, Boolean> isTruncatedFilter = k -> (k.getBoolean(k.fieldIndex("truncated")));
        Function<MappedTweet, Boolean> containsSentiment = k -> (!k.positiveWords.isEmpty() || !k.negativeWords.isEmpty());

        Function<Row, MappedTweet> mapToMappedTweet = row -> {
            long id = row.getLong(row.fieldIndex("id"));

            boolean isRetweet = !row.isNullAt(row.fieldIndex("retweeted_status"));
            boolean isTruncated = row.getBoolean(row.fieldIndex("truncated"));

            Row extendedTweetRow = row.getAs(row.fieldIndex("extended_tweet"));
            Row userRow = row.getAs(row.fieldIndex("user"));
            Row entitiesRow;
            if (isTruncated) {
                entitiesRow = extendedTweetRow.getAs(extendedTweetRow.fieldIndex("entities"));
            } else {
                entitiesRow = row.getAs(row.fieldIndex("entities"));
            }

            String text = row.getString(row.fieldIndex("text"));
            if (isTruncated) {
                text = extendedTweetRow.getString(extendedTweetRow.fieldIndex("full_text"));
            }
            String timestampMs = row.getString(row.fieldIndex("timestamp_ms"));

            long friendsCount = userRow.getLong(userRow.fieldIndex("friends_count"));
            boolean hasMentioned = !entitiesRow.getList(entitiesRow.fieldIndex("user_mentions")).isEmpty();

            text = text.replace(lineSeparator, " ");
            SentimentLists wordsLists = SentimentService.MapWords(text);

            return new MappedTweet(id, text, timestampMs, wordsLists.words, wordsLists.positiveWords, wordsLists.negativeWords, friendsCount, hasMentioned);
        };

        JavaRDD<MappedTweet> rdd = rows
//                .limit(100)
                .filter(rows.col("lang").like("en"))
                .javaRDD()
                .filter(isNotRetweetFilter)
                .map(mapToMappedTweet)
                .filter(containsSentiment);

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
