/**
 * SentimentAnalysisDriver Class
 *
 * <p>This is the driver class for the Hadoop MapReduce sentiment analysis job.
 * It's responsible for configuring and executing the entire MapReduce job flow.
 * The class sets up input/output paths, configures Mapper and Reducer classes,
 * and manages job execution.</p>
 *
 * <p>The driver performs the following operations:
 * <ul>
 *   <li>Parses command line arguments for input/output paths and optional wordlist paths</li>
 *   <li>Configures job parameters including input/output formats and types</li>
 *   <li>Sets up the Mapper and Reducer classes</li>
 *   <li>Manages output directory (deletes if it exists)</li>
 *   <li>Submits the job and monitors its execution</li>
 * </ul></p>
 *
 * <p>Usage:
 * <pre>hadoop jar hadoop-sentiment-analysis-1.0-SNAPSHOT.jar com.example.SentimentAnalysisDriver 
 * &lt;input_path&gt; &lt;output_path&gt; [positive_wordlist] [negative_wordlist]</pre></p>
 *
 * <p>Where:
 * <ul>
 *   <li>&lt;input_path&gt;: Path to the input data files</li>
 *   <li>&lt;output_path&gt;: Path where output will be written</li>
 *   <li>[positive_wordlist]: (Optional) Path to file containing positive words</li>
 *   <li>[negative_wordlist]: (Optional) Path to file containing negative words</li>
 * </ul></p>
 *
 * @author Hadoop Sentiment Analysis Team
 * @version 1.0
 */
package com.example;

import java.io.IOException;
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
 * SentimentAnalysisDriver configures and executes the Hadoop MapReduce job for
 * sentiment analysis.
 * It extends Configured and implements Tool to leverage Hadoop's command-line
 * parsing capabilities.
 */
public class SentimentAnalysisDriver extends Configured implements Tool {

    /**
     * Runs the MapReduce job with the specified arguments.
     * This method handles the core job configuration and execution.
     *
     * @param args Command-line arguments array with input/output paths and optional
     *             wordlist paths
     * @return 0 if the job completes successfully, 1 otherwise
     * @throws Exception If an error occurs during job configuration or execution
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(
                    "Usage: SentimentAnalysisDriver <input path> <output path> [positive wordlist] [negative wordlist]");
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
        FileSystem fs = getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Create and configure job
        Job job = createJob(conf, "Sentiment Analysis");

        // Set input and output paths
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Submit job and wait for completion
        boolean success = waitForCompletion(job);
        return success ? 0 : 1;
    }

    /**
     * Creates and configures a Job with the specified configuration.
     * This method is protected to allow for overriding in tests.
     *
     * @param conf The job configuration
     * @param name The name of the job
     * @return A configured Job instance
     * @throws IOException If there is an error creating the job
     */
    protected Job createJob(Configuration conf, String name) throws IOException {
        Job job = Job.getInstance(conf, name);
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

        return job;
    }

    /**
     * Waits for job completion. This method is protected to allow for overriding in
     * tests.
     *
     * @param job The job to wait for
     * @return true if the job completes successfully, false otherwise
     * @throws IOException            If there is an error during job execution
     * @throws InterruptedException   If the job is interrupted
     * @throws ClassNotFoundException If a class is not found during job execution
     */
    protected boolean waitForCompletion(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }

    /**
     * Gets a FileSystem instance for the given configuration.
     * This method is protected to allow for overriding in tests.
     *
     * @param conf The configuration to use
     * @return A FileSystem instance
     * @throws IOException If there is an error getting the file system
     */
    protected FileSystem getFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }

    /**
     * Main method to execute the Hadoop MapReduce job.
     * This is the entry point for the application when executed from the command
     * line.
     *
     * @param args Command-line arguments passed to the application
     * @throws Exception If an error occurs during job execution
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SentimentAnalysisDriver(), args);
        System.exit(exitCode);
    }
}