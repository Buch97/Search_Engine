package it.unipi.utils.textProcessing;

import org.tartarus.snowball.ext.PorterStemmer;

public class Stemmer {
    public static String stemming(String word){
        PorterStemmer stem = new PorterStemmer(); //Initialize the stemmer

        stem.setCurrent(word);
        stem.stem();

        //Return the "stemmed" word
        return stem.getCurrent();
    }
}
