package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the SentimentReducer class.
 */
public class SentimentReducerTest {

    private SentimentReducer reducer;
    private ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;

    @BeforeEach
    public void setUp() {
        reducer = new SentimentReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReduceWithSingleValue() throws IOException {
        // Test with a single value - should return that value
        String key = "PositiveWordCount";
        List<Integer> values = Arrays.asList(10);

        TestUtils.runReducerTest(reduceDriver, key, values, key, 10.0);
    }

    @Test
    public void testReduceWithMultipleValues() throws IOException {
        // Test with multiple values - should return average
        String key = "PositiveScore";
        List<Integer> values = Arrays.asList(80, 60, 70, 90);

        // Average = (80+60+70+90)/4 = 75.0
        TestUtils.runReducerTest(reduceDriver, key, values, key, 75.0);
    }

    @Test
    public void testReduceWithDecimalAverage() throws IOException {
        // Test with values resulting in a decimal average
        String key = "SentimentRatio";
        List<Integer> values = Arrays.asList(10, 20, 15);

        // Average = (10+20+15)/3 = 15.0
        TestUtils.runReducerTest(reduceDriver, key, values, key, 15.0);
    }

    @Test
    public void testReduceWithNoValues() throws IOException {
        // Test with no values - should return 0
        String key = "NegativeWordCount";
        List<Integer> values = new ArrayList<>();

        // Convert integers to IntWritable objects
        List<IntWritable> writableValues = new ArrayList<>();
        for (Integer value : values) {
            writableValues.add(new IntWritable(value));
        }

        // Setup and run the test manually since our utility expects at least one value
        reduceDriver.withInput(new Text(key), writableValues)
                .withOutput(new Text(key), new DoubleWritable(0.0))
                .runTest();
    }

    @Test
    public void testReduceWithNegativeValues() throws IOException {
        // Test with negative values - should calculate correct average
        String key = "SentimentRatio";
        List<Integer> values = Arrays.asList(50, -50, 25, -25);

        // Average = (50+(-50)+25+(-25))/4 = 0.0
        TestUtils.runReducerTest(reduceDriver, key, values, key, 0.0);
    }

    @Test
    public void testReduceRounding() throws IOException {
        // Test rounding to 2 decimal places
        String key = "PositiveScore";
        List<Integer> values = Arrays.asList(33, 34, 33);

        // Average = (33+34+33)/3 = 33.33... → should round to 33.33
        TestUtils.runReducerTest(reduceDriver, key, values, key, 33.33);
    }
}