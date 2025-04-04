/**
 * SentimentReducer Class
 *
 * <p>This is a Hadoop MapReduce Reducer implementation for sentiment analysis.
 * The class aggregates sentiment metrics emitted by SentimentMapper and calculates 
 * average values for each metric. It works with the following metrics:</p>
 *
 * <ul>
 *   <li>PositiveWordCount: Total count of positive words</li>
 *   <li>NegativeWordCount: Total count of negative words</li>
 *   <li>PositiveScore: Percentage of positive words among sentiment words</li>
 *   <li>NegativeScore: Percentage of negative words among sentiment words</li>
 *   <li>SentimentRatio: Ratio of positive to negative sentiment</li>
 * </ul>
 *
 * <p>The reducer performs the following operations:
 * <ul>
 *   <li>Aggregates values for each sentiment metric across all mappers</li>
 *   <li>Calculates average values for each metric</li>
 *   <li>Outputs results formatted to two decimal places</li>
 * </ul></p>
 *
 * <p>Output is formatted as key-value pairs where:
 * <ul>
 *   <li>Key: The sentiment metric name</li>
 *   <li>Value: The average value for that metric across all inputs</li>
 * </ul></p>
 *
 * @author Hadoop Sentiment Analysis Team
 * @version 1.0
 */
package com.example;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * SentimentReducer aggregates and calculates the average values for sentiment
 * metrics.
 */
public class SentimentReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    /**
     * A reusable DoubleWritable for output values to avoid object creation
     * overhead.
     */
    private final DoubleWritable result = new DoubleWritable();

    /**
     * Reduces the values for each sentiment metric by calculating their average.
     * The method processes all values for a given key (metric name) and computes
     * the average value across all mappers.
     *
     * @param key     The input key - the sentiment metric name
     * @param values  The collection of values for this key from all mappers
     * @param context The context object used to write output
     * @throws IOException          If there is an error writing to the context
     * @throws InterruptedException If the reduce operation is interrupted
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        // Calculate sum and count for average
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        // Calculate average metric value across all mappers
        double average = count > 0 ? (double) sum / count : 0.0;

        // Format the output to 2 decimal places
        average = Math.round(average * 100.0) / 100.0;

        result.set(average);
        context.write(key, result);
    }
}