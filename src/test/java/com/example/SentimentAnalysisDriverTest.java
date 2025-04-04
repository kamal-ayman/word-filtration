package com.example;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Unit tests for the SentimentAnalysisDriver class.
 */
public class SentimentAnalysisDriverTest {

    private SentimentAnalysisDriver driver;
    private Configuration conf;

    @BeforeEach
    public void setUp() {
        driver = new SentimentAnalysisDriver();
        conf = new Configuration();
        driver.setConf(conf);
    }

    @Test
    public void testInvalidArgs() throws Exception {
        // Test with insufficient arguments
        String[] args = new String[1];
        args[0] = "/input";

        int result = driver.run(args);

        // Should return error code (-1) with insufficient args
        assertEquals(-1, result);
    }

    @Test
    public void testJobConfiguration() throws Exception {
        // Create a custom SentimentAnalysisDriver subclass for testing
        SentimentAnalysisDriver testDriver = new SentimentAnalysisDriver() {
            @Override
            protected Job createJob(Configuration conf, String name) throws IOException {
                Job job = super.createJob(conf, name);

                // Verify job configuration
                assertEquals("Sentiment Analysis", job.getJobName());
                try {
                    assertEquals(SentimentMapper.class, job.getMapperClass());
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                try {
                    assertEquals(SentimentReducer.class, job.getReducerClass());
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                assertEquals(Text.class, job.getMapOutputKeyClass());
                assertEquals(IntWritable.class, job.getMapOutputValueClass());
                assertEquals(Text.class, job.getOutputKeyClass());
                assertEquals(DoubleWritable.class, job.getOutputValueClass());

                return job;
            }

            @Override
            protected boolean waitForCompletion(Job job)
                    throws IOException, InterruptedException, ClassNotFoundException {
                // Skip actual job execution
                return true;
            }
        };

        // Set configuration and run
        testDriver.setConf(conf);
        String[] args = { "/input", "/output" };

        int result = testDriver.run(args);

        // Should return success code (0)
        assertEquals(0, result);
    }

    @Test
    public void testWordlistConfiguration(@TempDir java.nio.file.Path tempDir) throws Exception {
        // Create temporary wordlist files
        java.nio.file.Path positiveFile = tempDir.resolve("positive.txt");
        java.nio.file.Path negativeFile = tempDir.resolve("negative.txt");
        Files.write(positiveFile, "good\ngreat".getBytes(StandardCharsets.UTF_8));
        Files.write(negativeFile, "bad\nterrible".getBytes(StandardCharsets.UTF_8));

        // Create a custom SentimentAnalysisDriver for testing
        SentimentAnalysisDriver testDriver = new SentimentAnalysisDriver() {
            @Override
            protected Job createJob(Configuration conf, String name) throws IOException {
                Job job = super.createJob(conf, name);

                // Verify wordlist configuration
                assertEquals(positiveFile.toString(), conf.get(SentimentMapper.POSITIVE_WORDLIST_PATH));
                assertEquals(negativeFile.toString(), conf.get(SentimentMapper.NEGATIVE_WORDLIST_PATH));

                return job;
            }

            @Override
            protected boolean waitForCompletion(Job job)
                    throws IOException, InterruptedException, ClassNotFoundException {
                // Skip actual job execution
                return true;
            }

            @Override
            protected FileSystem getFileSystem(Configuration conf) throws IOException {
                // Return a mock file system
                return new MockFileSystem();
            }

            // Simple mock file system class
            class MockFileSystem extends FileSystem {
                @Override
                public boolean exists(Path f) {
                    return false;
                }

                @Override
                public boolean delete(Path f, boolean recursive) {
                    return recursive;
                }

                @Override
                public Path getWorkingDirectory() {
                    return null;
                }

                @Override
                public void setWorkingDirectory(Path dir) {
                    // Do nothing
                }

                @Override
                public boolean delete(Path path) throws IOException {
                    return false;
                }

                @Override
                public boolean mkdirs(Path path) throws IOException {
                    return false;
                }

                @Override
                public FSDataInputStream open(Path path) throws IOException {
                    return null;
                }

                @Override
                public FSDataOutputStream create(Path path) throws IOException {
                    return null;
                }

                @Override
                public FSDataOutputStream append(Path path) throws IOException {
                    return null;
                }

                @Override
                public FileStatus getFileStatus(Path path) throws IOException {
                    return null;
                }

                @Override
                public FileStatus[] listStatus(Path path) throws IOException {
                    return new FileStatus[0];
                }

                @Override
                public URI getUri() {
                    return null;
                }

                @Override
                public FSDataInputStream open(Path f, int bufferSize) throws IOException {
                    // TODO Auto-generated method stub
                    throw new UnsupportedOperationException("Unimplemented method 'open'");
                }

                @Override
                public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                        short replication, long blockSize, Progressable progress) throws IOException {
                    // TODO Auto-generated method stub
                    throw new UnsupportedOperationException("Unimplemented method 'create'");
                }

                @Override
                public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
                    // TODO Auto-generated method stub
                    throw new UnsupportedOperationException("Unimplemented method 'append'");
                }

                @Override
                public boolean rename(Path src, Path dst) throws IOException {
                    // TODO Auto-generated method stub
                    throw new UnsupportedOperationException("Unimplemented method 'rename'");
                }

                @Override
                public boolean mkdirs(Path f, FsPermission permission) throws IOException {
                    // TODO Auto-generated method stub
                    throw new UnsupportedOperationException("Unimplemented method 'mkdirs'");
                }
            }
        };

        // Set configuration and run
        testDriver.setConf(conf);
        String[] args = { "/input", "/output", positiveFile.toString(), negativeFile.toString() };

        int result = testDriver.run(args);

        // Should return success code (0)
        assertEquals(0, result);
    }
}