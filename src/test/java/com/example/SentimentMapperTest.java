package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the SentimentMapper class.
 */
public class SentimentMapperTest {

    private SentimentMapper mapper;
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @BeforeEach
    public void setUp() throws IOException {
        mapper = new SentimentMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        // Create and set paths for positive and negative word lists
        System.setProperty(SentimentMapper.POSITIVE_WORDLIST_PATH, TestUtils.createPositiveWordsFile());
        System.setProperty(SentimentMapper.NEGATIVE_WORDLIST_PATH, TestUtils.createNegativeWordsFile());
    }

    @Test
    public void testMapWithNoSentimentWords() throws IOException {
        String text = "This is a neutral text with no sentiment words";

        // No outputs expected since there are no sentiment words
        mapDriver.withInput(new LongWritable(1), new Text(text));
        mapDriver.runTest();
        assertEquals(0, mapDriver.getCounters()
                .findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue());
    }

    @Test
    public void testMapWithPositiveWords() throws IOException {
        String text = "This is good and great content, truly excellent work";

        List<TestUtils.KeyValuePair<String, Integer>> expectedOutputs = new ArrayList<>();
        expectedOutputs.add(new TestUtils.KeyValuePair<>("PositiveWordCount", 3));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("NegativeWordCount", 0));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("PositiveScore", 100));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("NegativeScore", 0));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("SentimentRatio", 100));

        TestUtils.runMapperTest(mapDriver, text, expectedOutputs);
    }

    @Test
    public void testMapWithNegativeWords() throws IOException {
        String text = "This is bad and terrible content, truly awful work";

        List<TestUtils.KeyValuePair<String, Integer>> expectedOutputs = new ArrayList<>();
        expectedOutputs.add(new TestUtils.KeyValuePair<>("PositiveWordCount", 0));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("NegativeWordCount", 3));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("PositiveScore", 0));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("NegativeScore", 100));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("SentimentRatio", -100));

        TestUtils.runMapperTest(mapDriver, text, expectedOutputs);
    }

    @Test
    public void testMapWithMixedSentiment() throws IOException {
        String text = "This has both good and bad parts, excellent but terrible";

        List<TestUtils.KeyValuePair<String, Integer>> expectedOutputs = new ArrayList<>();
        expectedOutputs.add(new TestUtils.KeyValuePair<>("PositiveWordCount", 2));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("NegativeWordCount", 2));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("PositiveScore", 50));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("NegativeScore", 50));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("SentimentRatio", 0));

        TestUtils.runMapperTest(mapDriver, text, expectedOutputs);
    }

    @Test
    public void testMapWithPunctuationAndCapitalization() throws IOException {
        String text = "This is GOOD! And great, content; truly excellent. work?";

        List<TestUtils.KeyValuePair<String, Integer>> expectedOutputs = new ArrayList<>();
        expectedOutputs.add(new TestUtils.KeyValuePair<>("PositiveWordCount", 3));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("NegativeWordCount", 0));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("PositiveScore", 100));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("NegativeScore", 0));
        expectedOutputs.add(new TestUtils.KeyValuePair<>("SentimentRatio", 100));

        TestUtils.runMapperTest(mapDriver, text, expectedOutputs);
    }

    @Test
    public void testInvalidWordlistPaths() {
        // Set invalid paths
        System.setProperty(SentimentMapper.POSITIVE_WORDLIST_PATH, "/invalid/path/positive.txt");
        System.setProperty(SentimentMapper.NEGATIVE_WORDLIST_PATH, "/invalid/path/negative.txt");

        // Create a new mapper which should fail on setup
        mapper = new SentimentMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        // Verify that the IOException is thrown during setup
        assertThrows(IOException.class, () -> {
            mapDriver.withInput(new LongWritable(1), new Text("test"))
                    .runTest();
        });
    }
}