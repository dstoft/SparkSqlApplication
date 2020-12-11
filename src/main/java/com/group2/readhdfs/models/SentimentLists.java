package com.group2.readhdfs.models;

import java.util.ArrayList;

public class SentimentLists {

    public final ArrayList<String> words;
    public final ArrayList<String> positiveWords;
    public final ArrayList<String> negativeWords;
    public final String newText;

    public SentimentLists(ArrayList<String> words, ArrayList<String> positiveWords, ArrayList<String> negativeWords, String newText) {
        this.words = words;
        this.positiveWords = positiveWords;
        this.negativeWords = negativeWords;
        this.newText = newText;
    }

}
