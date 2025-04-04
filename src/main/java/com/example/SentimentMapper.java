/**
 * SentimentMapper Class
 * 
 * <p>This is a Hadoop MapReduce Mapper implementation for sentiment analysis.
 * The class processes text input to identify positive and negative sentiments
 * based on predefined wordlists. It emits various metrics related to sentiment
 * analysis including word counts, scores, and ratios.</p>
 * 
 * <p>The mapper performs the following functions:
 * <ul>
 *   <li>Loads positive and negative words from configurable wordlist files</li>
 *   <li>Tokenizes input text and counts positive/negative words</li>
 *   <li>Calculates sentiment metrics (scores, ratios)</li>
 *   <li>Emits key-value pairs for these metrics</li>
 * </ul></p>
 * 
 * <p>Configuration:
 * <ul>
 *   <li>POSITIVE_WORDLIST_PATH: Path to file containing positive words</li>
 *   <li>NEGATIVE_WORDLIST_PATH: Path to file containing negative words</li>
 * </ul></p>
 * 
 * <p>Output metrics:
 * <ul>
 *   <li>PositiveWordCount: Total count of positive words</li>
 *   <li>NegativeWordCount: Total count of negative words</li>
 *   <li>PositiveScore: Percentage of positive words among sentiment words</li>
 *   <li>NegativeScore: Percentage of negative words among sentiment words</li>
 *   <li>SentimentRatio: Ratio of positive to negative sentiment</li>
 * </ul></p>
 * 
 * @author Hadoop Sentiment Analysis Team
 * @version 1.0
 */
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

/**
 * SentimentMapper processes each line of text input to calculate sentiment
 * metrics.
 */
public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Default wordlist paths
    /**
     * Default path to the file containing positive words.
     */
    private static final String DEFAULT_POSITIVE_PATH = "input/positive.txt";

    /**
     * Default path to the file containing negative words.
     */
    private static final String DEFAULT_NEGATIVE_PATH = "input/negative.txt";

    // Constants for configuration properties
    /**
     * Configuration property for the path to positive wordlist file.
     * Uses environment variable or falls back to default path.
     */
    public static final String POSITIVE_WORDLIST_PATH = System.getenv().getOrDefault("POSITIVE_WORDLIST_PATH",
            DEFAULT_POSITIVE_PATH);

    /**
     * Configuration property for the path to negative wordlist file.
     * Uses environment variable or falls back to default path.
     */
    public static final String NEGATIVE_WORDLIST_PATH = System.getenv().getOrDefault("NEGATIVE_WORDLIST_PATH",
            DEFAULT_NEGATIVE_PATH);

    // Output keys as constants
    /**
     * Key for the count of positive words found in the text.
     */
    private static final Text POSITIVE_COUNT_KEY = new Text("PositiveWordCount");

    /**
     * Key for the count of negative words found in the text.
     */
    private static final Text NEGATIVE_COUNT_KEY = new Text("NegativeWordCount");

    /**
     * Key for the positive score (percentage of positive words).
     */
    private static final Text POSITIVE_SCORE_KEY = new Text("PositiveScore");

    /**
     * Key for the negative score (percentage of negative words).
     */
    private static final Text NEGATIVE_SCORE_KEY = new Text("NegativeScore");

    /**
     * Key for the sentiment ratio (ratio of positive to negative sentiment).
     */
    private static final Text SENTIMENT_RATIO_KEY = new Text("SentimentRatio");

    /**
     * Set containing all positive words loaded from the wordlist file.
     */
    private final Set<String> positiveWords = new HashSet<>();

    /**
     * Set containing all negative words loaded from the wordlist file.
     */
    private final Set<String> negativeWords = new HashSet<>();

    /**
     * Reusable IntWritable for emitting values to the reducer.
     */
    private final IntWritable outputValue = new IntWritable();

    /**
     * Sets up the mapper by loading the positive and negative wordlists.
     * This method is called once at the beginning of the task.
     *
     * @param context The context object for this task
     * @throws IOException          If there is an error reading the wordlist files
     * @throws InterruptedException If the task is interrupted
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            // Load positive and negative wordlists
            loadWordSet(positiveWords, POSITIVE_WORDLIST_PATH);
            loadWordSet(negativeWords, NEGATIVE_WORDLIST_PATH);
        } catch (IOException e) {
            throw new IOException("Error loading wordlists: " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid character encoding in wordlists: " + e.getMessage(), e);
        }
    }

    /**
     * Loads words from a file into the provided Set.
     * Each line in the file is considered a separate word.
     * Words are trimmed and converted to lowercase.
     * 
     * @param wordSet  The Set to load the words into
     * @param filePath Path to the file containing the words
     * @throws IOException If there is an error reading the file
     */
    private void loadWordSet(Set<String> wordSet, String filePath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
            reader.lines()
                    .map(String::trim)
                    .map(String::toLowerCase)
                    .filter(this::isValidWord)
                    .forEach(wordSet::add);
        }
    }

    /**
     * Validates if a word from the wordlist should be included.
     * Words that are empty or start with "//" are excluded.
     * 
     * @param line The word to validate
     * @return true if the word is valid, false otherwise
     */
    private boolean isValidWord(String line) {
        return !line.isEmpty() && !line.startsWith("//");
    }

    /**
     * Processes a single line of text input to calculate sentiment metrics.
     * The method tokenizes the input text, counts positive and negative words,
     * calculates sentiment scores and ratios, and emits these metrics.
     *
     * @param key     The line offset in the input file
     * @param value   The line text from the input file
     * @param context The context object for this task
     * @throws IOException          If there is an error processing the input
     * @throws InterruptedException If the task is interrupted
     */
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

    /**
     * Helper method to emit a key-value pair to the context.
     * This method simplifies the process of setting and emitting values.
     *
     * @param context The context object to emit to
     * @param key     The key to emit
     * @param value   The integer value to emit
     * @throws IOException          If there is an error emitting the value
     * @throws InterruptedException If the task is interrupted
     */
    private void emitValue(Context context, Text key, int value) throws IOException, InterruptedException {
        outputValue.set(value);
        context.write(key, outputValue);
    }
}