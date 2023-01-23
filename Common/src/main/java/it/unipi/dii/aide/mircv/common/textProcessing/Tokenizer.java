package it.unipi.dii.aide.mircv.common.textProcessing;

import it.unipi.dii.aide.mircv.common.utils.Flags;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Tokenizer {
    private final String bodyText;
    public static int doc_len;
    final Map<String, Integer> token_list = new HashMap<>();
    BufferedReader bufferedReader = new BufferedReader(new FileReader("resources/stopwords/stopwords.txt"));
    final String[] STOPWORDS;


    public Tokenizer(String bodyText) throws IOException {
        this.bodyText = bodyText;
        STOPWORDS = bufferedReader.readLine().split(" ");
    }

    public Map<String, Integer> tokenize() {

        String text = bodyText.toLowerCase();

        /*
        //Remove punctuation
        text = text.replaceAll("\\p{Punct}", "");
        //Remove non-ascii chars
        text = text.replaceAll("[^\\x00-\\x7F]", "");
        //Remove useless whitespaces (starting-ending and double+)
        text = text.trim().replaceAll(" +"," ");
        */
        /*text = text.replaceAll("[\\\\$%{}\\[\\]()`<>='&°»§£€:,;/.~*|\"^_\\-+!?#\t@]","");
        text = text.replaceAll("[^a-zA-Z0-9]", " ");*/

        //remove urls, if any
        text = text.replaceAll("[(http(s)?):\\/\\/(www\\.)?a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)", "\s");

        //remove html tags, if any
        text = text.replaceAll("<[^>]+>", "\s");

        //remove non-digit characters including punctuation
        text = text.replaceAll("[^a-zA-Z ]", "\s");

        //collapse 3+ repeating characters in just 2
        text = text.replaceAll("(.)\\1{2,}","$1$1");

        //Remove non-ascii chars
        text = text.replaceAll("[^\\x00-\\x7F]", "");

        //Remove useless whitespaces (starting-ending and double+)
        text = text.trim().replaceAll(" +"," ");

        extractToken(text);

        return token_list;
    }

    public void extractToken(final String text) {
        //space based tokenization
        final StringTokenizer normalTokenizer = new StringTokenizer(text, " ");
        doc_len = normalTokenizer.countTokens();
        while (normalTokenizer.hasMoreTokens()) {
            String word = normalTokenizer.nextToken().trim();

            if(Flags.isStopStem()) {
                if (word.length() > 0 && !Arrays.asList(STOPWORDS).contains(word)) {
                    word = Stemmer.stemming(word);
                    token_list.merge(word, 1, Integer::sum);
                }
            }
            else{
                token_list.merge(word, 1, Integer::sum);
            }
        }
    }
}


