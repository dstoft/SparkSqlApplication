package com.group2.readhdfs;

import com.group2.readhdfs.models.SentimentLists;

import java.util.ArrayList;
import java.util.Arrays;

public class SentimentService {


    private static final String[] positiveWords = new String[]{"happy", "positive", "good", "amazing", "awesome", "affirmative", "appealing", "attractive", "brilliant", "beneficial", "beautiful", "bliss", "brave", "calm", "cool", "cute", "courageous", "commend", "delight", "divine", "great"};
    private static final String[] negativeWords = new String[]{"deaths", "unhappy", "negative", "bad", "fuck", "shit", "alarming", "angry", "awful", "collapse", "cruel", "corrupt", "damage", "damaging", "despicable", "distress", "depressed", "disgusting", "fast-spreading", "terrifying"};
    private static final ArrayList<String> positiveWordsList = new ArrayList<>(Arrays.asList(positiveWords));
    private static final ArrayList<String> negativeWordsList = new ArrayList<>(Arrays.asList(negativeWords));


    public static SentimentLists MapWords(String text) {
        String newText = CleanWordsString(text);
        ArrayList<String> words = new ArrayList<>(Arrays.asList(newText.split(" ")));
        ArrayList<String> positiveWords = GetCommonElements(words, positiveWordsList);
        ArrayList<String> negativeWords = GetCommonElements(words, negativeWordsList);

        return new SentimentLists(words, positiveWords, negativeWords, newText);
    }

    private static String CleanWordsString(String text) {
        String newText = text;
        // https://javarevisited.blogspot.com/2016/02/how-to-remove-all-special-characters-of-String-in-java.html
        newText = text.replaceAll("[^a-zA-Z0-9@ _-]", "");
        newText = newText.toLowerCase();
        return newText;
    }

    private static ArrayList<String> GetCommonElements(ArrayList<String> listOne, ArrayList<String> listTwo) {
        ArrayList<String> newListOne = (ArrayList<String>) listOne.clone();
        newListOne.retainAll(listTwo);
        return newListOne;
    }


}
