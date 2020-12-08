package com.group2.readhdfs.models;

import java.io.Serializable;
import java.util.ArrayList;

public class MappedTweet implements Serializable {
    public long id;
    public String text;
    public long timeInMs;
    public ArrayList<String> words;

    public MappedTweet(long id, String text, long timeInMs, ArrayList<String> words) {
        this.id = id;
        this.text = text;
        this.timeInMs = timeInMs;
        this.words = words;
    }

    public MappedTweet(long id, String text, String timeInMs, ArrayList<String> words) throws NumberFormatException {
        this.id = id;
        this.text = text;
        this.timeInMs = MapMsTimeString(timeInMs);
        this.words = words;
    }

    private long MapMsTimeString(String timeInMsStr) throws NumberFormatException {
        return Long.parseLong(timeInMsStr);
    }

    @Override
    public String toString() {
        return "{" +
                "id:" + id +
                ", text:'" + text + '\'' +
                ", timeInMs:" + timeInMs +
                ", words:" + words +
                '}';
    }
}
