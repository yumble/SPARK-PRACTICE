package com.example.sparkexcercise;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class Util
{
    public static Set<String> borings = new HashSet<String>();

    static {
        // On the video, I say "change this class name" - ignore that.
        // The class name should have been Util, as it is here!
        InputStream is = Util.class.getResourceAsStream("/subtitles/boringwords.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        br.lines().forEach(borings::add);
    }

    /**
     * Returns true if we think the word is "boring" - ie it doesn't seem to be a keyword
     * for a training course.
     */
    public static boolean isBoring(String word)
    {
        return borings.contains(word);
    }

    /**
     * Convenience method for more readable client code
     */
    public static boolean isNotBoring(String word)
    {
        return !isBoring(word);
    }

}
