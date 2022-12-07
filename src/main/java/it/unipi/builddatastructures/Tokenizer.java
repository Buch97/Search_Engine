package it.unipi.builddatastructures;

import java.util.*;

public class Tokenizer {
    private final String bodyText;
    final Map<String, Integer> token_list = new HashMap<>();
    final String PATTERN_TOKEN = "\\$%{}[]()`<>='&:,;/.~ *\n|\"^_-+!?#\t@";
    final String[] STOPWORDS = {"a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
            "there", "these", "they", "this", "to", "was", "will", "with"};


    public Tokenizer(String bodyText) {
        this.bodyText = bodyText;
    }

    public Map<String, Integer> tokenize() {
        // QUESTI REPLACE LI AVEVO COPIATI DA UN CODICE CHE AVEVO VISTO, ALCUNI POSSONO SERVIRE, ALTRI MAGARI NO
        // E ALTRI VANNO AGGIUNTI
        String text = bodyText.toLowerCase();

        text = text.replaceAll("[\\\\$%{}\\[\\]()`<>='&°»§£€:,;/.~*|\"^_\\-+!?#\t@]","");
        text = text.replaceAll("<ref>.*?</ref>", "");
        text = text.replaceAll("</?.*?>", "");
        text = text.replaceAll("\\{\\{.*?}}", "");
        text = text.replaceAll("\\[\\[.*?:.*?]]", "");
        text = text.replaceAll("\\[\\[(.*?)]]", "");
        text = text.replaceAll("\\s(.*?)\\|(\\w+\\s)", " $2");
        text = text.replaceAll("\\[.*?]", " ");
        text = text.replaceAll("(?s)<!--.*?-->", "");
        text = text.replaceAll("\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", "");

        extractToken(text);
        return token_list;
    }

    public void extractToken(final String text) {
        //final StringTokenizer normalTokenizer = new StringTokenizer(text, PATTERN_TOKEN);
        //space based tokenization
        final StringTokenizer normalTokenizer = new StringTokenizer(text, " ");
        while (normalTokenizer.hasMoreTokens()) {
            String word = normalTokenizer.nextToken().trim();
            //word = word.replaceAll("[\\\\$%{}\\[\\]()`<>='&°§£€:,;/.~*|\"^_\\-+!?#\t@]","");
            if (word.length() > 0 && !Arrays.asList(STOPWORDS).contains(word)) {
                //se è la prima volta che si incontra si inserisce con valore 1 else si aumenta il valore di 1
                token_list.merge(word, 1, Integer::sum);
            }
        }
    }
}


