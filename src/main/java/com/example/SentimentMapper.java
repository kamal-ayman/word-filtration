package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Default wordlist paths
    private static final String DEFAULT_POSITIVE_PATH = "input/positive.txt";
    private static final String DEFAULT_NEGATIVE_PATH = "input/negative.txt";

    // Constants for configuration properties
    public static final String POSITIVE_WORDLIST_PATH = System.getenv().getOrDefault("POSITIVE_WORDLIST_PATH",
            DEFAULT_POSITIVE_PATH);
    public static final String NEGATIVE_WORDLIST_PATH = System.getenv().getOrDefault("NEGATIVE_WORDLIST_PATH",
            DEFAULT_NEGATIVE_PATH);

    // Output keys as constants
    private static final Text POSITIVE_COUNT_KEY = new Text("PositiveWordCount");
    private static final Text NEGATIVE_COUNT_KEY = new Text("NegativeWordCount");
    private static final Text POSITIVE_SCORE_KEY = new Text("PositiveScore");
    private static final Text NEGATIVE_SCORE_KEY = new Text("NegativeScore");
    private static final Text SENTIMENT_RATIO_KEY = new Text("SentimentRatio");

    private final Set<String> positiveWords = new HashSet<>();
    private final Set<String> negativeWords = new HashSet<>();
    private final IntWritable outputValue = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            System.out.println("Loading wordlists from: " + POSITIVE_WORDLIST_PATH + ", " + NEGATIVE_WORDLIST_PATH);
            // Load positive and negative wordlists
            loadWordSet(positiveWords, POSITIVE_WORDLIST_PATH);
            loadWordSet(negativeWords, NEGATIVE_WORDLIST_PATH);
        } catch (IOException e) {
            throw new IOException("Error loading wordlists: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid character encoding in wordlists: " + e.getMessage(), e);
        }
    }

    private void loadWordSet(Set<String> wordSet, String filePath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            reader.lines()
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .filter(this::isValidWord)
                    .forEach(wordSet::add);
        }
    }

    private boolean isValidWord(String line) {
        return !line.isEmpty() && !line.startsWith("//");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(line);

        int positiveCount = 0;
        int negativeCount = 0;

        // Count positive and negative words
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            word = word.replaceAll("[^a-zA-Z0-9-+]", ""); // Clean the word

            if (positiveWords.contains(word)) {
                positiveCount++;
            }
            if (negativeWords.contains(word)) {
                negativeCount++;
            }
        }

        // Skip processing if no sentiment words found
        int totalSentimentWords = positiveCount + negativeCount;
        if (totalSentimentWords == 0) {
            return;
        }

        // Calculate sentiment metrics
        int sentimentRatio = (int) (((double) (positiveCount - negativeCount) / totalSentimentWords) * 100);
        int positiveScore = (int) (((double) positiveCount / totalSentimentWords) * 100);
        int negativeScore = (int) (((double) negativeCount / totalSentimentWords) * 100);

        // Emit results
        emitValue(context, POSITIVE_COUNT_KEY, positiveCount);
        emitValue(context, NEGATIVE_COUNT_KEY, negativeCount);
        emitValue(context, POSITIVE_SCORE_KEY, positiveScore);
        emitValue(context, NEGATIVE_SCORE_KEY, negativeScore);
        emitValue(context, SENTIMENT_RATIO_KEY, sentimentRatio);
    }

    private void emitValue(Context context, Text key, int value) throws IOException, InterruptedException {
        outputValue.set(value);
        context.write(key, outputValue);
    }
}