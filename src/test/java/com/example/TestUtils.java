package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

/**
 * Utility class with helper methods for testing the MapReduce components.
 */
public class TestUtils {

    /**
     * Creates a temporary file with the given content.
     * 
     * @param content The content to write to the file
     * @return The path to the temporary file
     * @throws IOException If an error occurs while creating the file
     */
    public static String createTempFile(String content) throws IOException {
        java.nio.file.Path path = java.nio.file.Files.createTempFile("wordlist", ".txt");
        java.nio.file.Files.write(path, content.getBytes());
        return path.toAbsolutePath().toString();
    }

    /**
     * Creates a positive words file for testing.
     * 
     * @return The path to the positive words file
     * @throws IOException If an error occurs while creating the file
     */
    public static String createPositiveWordsFile() throws IOException {
        return createTempFile("good\ngreat\nexcellent\nawesome");
    }

    /**
     * Creates a negative words file for testing.
     * 
     * @return The path to the negative words file
     * @throws IOException If an error occurs while creating the file
     */
    public static String createNegativeWordsFile() throws IOException {
        return createTempFile("bad\nterrible\nawful\nhorrible");
    }

    /**
     * Runs a mapper test with the given input text and verifies expected outputs.
     * 
     * @param mapDriver       The map driver to run
     * @param inputText       The input text to process
     * @param expectedOutputs List of expected key-value pairs
     * @throws IOException If an error occurs during mapping
     */
    public static void runMapperTest(MapDriver<LongWritable, Text, Text, IntWritable> mapDriver,
            String inputText,
            List<KeyValuePair<String, Integer>> expectedOutputs) throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(inputText));

        for (KeyValuePair<String, Integer> pair : expectedOutputs) {
            mapDriver.withOutput(new Text(pair.key), new IntWritable(pair.value));
        }

        mapDriver.runTest();
    }

    /**
     * Runs a reducer test with the given inputs and verifies expected outputs.
     * 
     * @param reduceDriver  The reduce driver to run
     * @param inputKey      The input key
     * @param inputValues   List of input values
     * @param expectedKey   The expected output key
     * @param expectedValue The expected output value
     * @throws IOException If an error occurs during reducing
     */
    public static void runReducerTest(ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver,
            String inputKey,
            List<Integer> inputValues,
            String expectedKey,
            double expectedValue) throws IOException {
        List<IntWritable> values = new ArrayList<>();
        for (Integer value : inputValues) {
            values.add(new IntWritable(value));
        }

        reduceDriver.withInput(new Text(inputKey), values)
                .withOutput(new Text(expectedKey), new DoubleWritable(expectedValue))
                .runTest();
    }

    /**
     * Utility class to hold key-value pairs for testing.
     */
    public static class KeyValuePair<K, V> {
        public final K key;
        public final V value;

        public KeyValuePair(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }
}