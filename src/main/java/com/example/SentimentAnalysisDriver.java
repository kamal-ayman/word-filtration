package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class for the Sentiment Analysis MapReduce job.
 */
public class SentimentAnalysisDriver extends Configured implements Tool {
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SentimentAnalysisDriver <input path> <output path> [positive wordlist] [negative wordlist]");
            return -1;
        }

        // Get input and output paths
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        // Initialize configuration
        Configuration conf = getConf();
        
        // Set wordlist paths if provided
        if (args.length > 2) {
            conf.set(SentimentMapper.POSITIVE_WORDLIST_PATH, args[2]);
        }
        if (args.length > 3) {
            conf.set(SentimentMapper.NEGATIVE_WORDLIST_PATH, args[3]);
        }
        
        // Delete output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        
        // Create and configure job
        Job job = Job.getInstance(conf, "Sentiment Analysis");
        job.setJarByClass(getClass());
        
        // Set classes
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);
        
        // Configure input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Set output key/value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        // Set input and output paths
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // Submit job and wait for completion
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SentimentAnalysisDriver(), args);
        System.exit(exitCode);
    }
}