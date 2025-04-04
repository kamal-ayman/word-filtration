package com.example;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer that calculates average sentiment metrics across all mappers.
 */
public class SentimentReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    private final DoubleWritable result = new DoubleWritable();

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