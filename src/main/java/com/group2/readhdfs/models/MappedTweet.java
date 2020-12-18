package com.group2.readhdfs.models;

import java.util.ArrayList;

public class MappedTweet {
    public long id;
    public String text;
    public long timeInMs;
    public ArrayList<String> words;
    public ArrayList<String> positiveWords;
    public ArrayList<String> negativeWords;
    public long friendsCount;
    public boolean hasMentioned;
    public int sentimentScore;

    private MappedTweet(long id, String text, ArrayList<String> words, ArrayList<String> positiveWords,
                        ArrayList<String> negativeWords, long friendsCount, boolean hasMentioned, int sentimentScore) {
        this.id = id;
        this.text = text;
        this.words = words;
        this.positiveWords = positiveWords;
        this.negativeWords = negativeWords;
        this.friendsCount = friendsCount;
        this.hasMentioned = hasMentioned;
        this.sentimentScore = sentimentScore;
    }

    public MappedTweet(long id, String text, long timeInMs, ArrayList<String> words, ArrayList<String> positiveWords,
                       ArrayList<String> negativeWords, long friendsCount, boolean hasMentioned, int sentimentScore) {
        this(id, text, words, positiveWords, negativeWords, friendsCount, hasMentioned, sentimentScore);
        this.timeInMs = timeInMs;
    }

    public MappedTweet(long id, String text, String timeInMs, ArrayList<String> words, ArrayList<String> positiveWords,
                       ArrayList<String> negativeWords, long friendsCount, boolean hasMentioned, int sentimentScore) throws NumberFormatException {
        this(id, text, words, positiveWords, negativeWords, friendsCount, hasMentioned, sentimentScore);
        this.timeInMs = MapMsTimeString(timeInMs);
    }

    private long MapMsTimeString(String timeInMsStr) throws NumberFormatException {
        return Long.parseLong(timeInMsStr);
    }

    @Override
    public String toString() {
        String wordsJson = words.isEmpty() ? "[]" : "[\"" + String.join("\",\"", words) + "\"]";
        String positiveWordsJson = positiveWords.isEmpty() ? "[]" : "[\"" + String.join("\",\"", positiveWords) + "\"]";
        String negativeWordsJson = negativeWords.isEmpty() ? "[]" : "[\"" + String.join("\",\"", negativeWords) + "\"]";
        return "{" +
                "\"id\":" + id +
                ", \"text\":\"" + text + '"' +
                ", \"timeInMs\":" + timeInMs +
                ", \"words\":" + wordsJson +
                ", \"positiveWords\":" + positiveWordsJson +
                ", \"negativeWords\":" + negativeWordsJson +
                ", \"friendsCount\":" + friendsCount +
                ", \"hasMentioned\":" + hasMentioned +
                ", \"sentimentScore\":" + sentimentScore +
                '}';
    }
}
