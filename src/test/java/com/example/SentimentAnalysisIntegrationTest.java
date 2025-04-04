package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the complete Sentiment Analysis MapReduce flow.
 */
public class SentimentAnalysisIntegrationTest {

    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;

    @BeforeEach
    public void setUp() throws IOException {
        // Create the MapReduceDriver with both mapper and reducer
        SentimentMapper mapper = new SentimentMapper();
        SentimentReducer reducer = new SentimentReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        // Setup wordlist paths
        System.setProperty(SentimentMapper.POSITIVE_WORDLIST_PATH, TestUtils.createPositiveWordsFile());
        System.setProperty(SentimentMapper.NEGATIVE_WORDLIST_PATH, TestUtils.createNegativeWordsFile());
    }

    @Test
    public void testCompleteMapReduceFlow() throws IOException {
        // Test with multiple inputs with mixed sentiment
        List<String> inputs = new ArrayList<>();
        inputs.add("This is good and excellent content");
        inputs.add("This is bad and terrible content");
        inputs.add("This has good and bad parts");

        // Add all inputs
        for (int i = 0; i < inputs.size(); i++) {
            mapReduceDriver.withInput(new LongWritable(i), new Text(inputs.get(i)));
        }

        // Define expected outputs
        // Expected calculations:
        // PositiveWordCount: 3 (good, excellent, good) = 1.0 per input
        // NegativeWordCount: 3 (bad, terrible, bad) = 1.0 per input
        // PositiveScore: 100, 0, 50 = 50.0 avg
        // NegativeScore: 0, 100, 50 = 50.0 avg
        // SentimentRatio: 100, -100, 0 = 0.0 avg
        mapReduceDriver.withOutput(new Text("NegativeScore"), new DoubleWritable(50.0))
                .withOutput(new Text("NegativeWordCount"), new DoubleWritable(1.0))
                .withOutput(new Text("PositiveScore"), new DoubleWritable(50.0))
                .withOutput(new Text("PositiveWordCount"), new DoubleWritable(1.0))
                .withOutput(new Text("SentimentRatio"), new DoubleWritable(0.0));

        // Run the test
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduceWithNoSentimentWords() throws IOException {
        // When no sentiment words are found, mapper should not emit anything
        // So reducer should not receive any values
        mapReduceDriver.withInput(new LongWritable(1), new Text("This text has no sentiment words"))
                .runTest();

        // Verify no output records
        assertEquals(0, mapReduceDriver.getCounters()
                .findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue());
    }

    @Test
    public void testMapReduceWithOnlyPositiveSentiment() throws IOException {
        // Test with only positive sentiment words
        mapReduceDriver.withInput(new LongWritable(1), new Text("good excellent awesome great"))
                .withOutput(new Text("NegativeScore"), new DoubleWritable(0.0))
                .withOutput(new Text("NegativeWordCount"), new DoubleWritable(0.0))
                .withOutput(new Text("PositiveScore"), new DoubleWritable(100.0))
                .withOutput(new Text("PositiveWordCount"), new DoubleWritable(4.0))
                .withOutput(new Text("SentimentRatio"), new DoubleWritable(100.0))
                .runTest();
    }

    @Test
    public void testMapReduceWithOnlyNegativeSentiment() throws IOException {
        // Test with only negative sentiment words
        mapReduceDriver.withInput(new LongWritable(1), new Text("bad terrible awful horrible"))
                .withOutput(new Text("NegativeScore"), new DoubleWritable(100.0))
                .withOutput(new Text("NegativeWordCount"), new DoubleWritable(4.0))
                .withOutput(new Text("PositiveScore"), new DoubleWritable(0.0))
                .withOutput(new Text("PositiveWordCount"), new DoubleWritable(0.0))
                .withOutput(new Text("SentimentRatio"), new DoubleWritable(-100.0))
                .runTest();
    }
}