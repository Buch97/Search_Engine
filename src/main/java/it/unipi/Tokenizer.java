package it.unipi;

import java.util.*;

public class Tokenizer {
    private int doc_id;
    private String bodyText;
    private int bodyLength = 0;
    final Map<String, Integer> token_list = new HashMap<>();
    final String PATTERN_TOKEN = "\\$%{}[]()`<>='&:,;/.~ ;*\n|\"^_-+!?#\t@";
    final String[] STOPWORDS = {"a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
            "there", "these", "they", "this", "to", "was", "will", "with"};

    public Tokenizer(int doc_id, String bodyText) {
        this.doc_id = doc_id;
        this.bodyText = bodyText;
    }

    public int getDocId() {
        return doc_id;
    }

    public void setDocId(int id) {
        this.doc_id = id;
    }

    public String getBodyText() {
        return bodyText;
    }

    public void setBodyText(final String text) {
        this.bodyText = text;
    }

    public int getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }

    public Map<String, Integer> tokenize() {
        //Remove the useless tokens.
        String text = bodyText.toLowerCase();
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
        //Use PATTERN_TOKEN to tokenize the text.
        final StringTokenizer normalTokenizer = new StringTokenizer(text, PATTERN_TOKEN);
        while (normalTokenizer.hasMoreTokens()) {
            String word = normalTokenizer.nextToken().trim();
            if (word.length() > 0 && !Arrays.asList(STOPWORDS).contains(word)) {
                token_list.merge(word, 1, Integer::sum);
            }
        }
    }
}
