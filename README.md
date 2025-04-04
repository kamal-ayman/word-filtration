# Sentiment Analysis with Hadoop MapReduce

## Overview
This project implements a sentiment analysis tool using Hadoop MapReduce. The `SentimentMapper` class processes text data to calculate sentiment metrics such as positive and negative word counts, sentiment scores, and sentiment ratios. The tool uses predefined wordlists for positive and negative words to perform the analysis.

## Features
- Counts occurrences of positive and negative words in the input text.
- Calculates sentiment scores and ratios based on the word counts.
- Configurable wordlist paths via environment variables.

## File Structure
```
/home/hdoop/demo
├── src
│   └── main
│       └── java
│           └── com
│               └── example
│                   └── SentimentMapper.java
└── input
    ├── positive.txt
    └── negative.txt
```

## Setup
1. **Prerequisites**:
   - Java Development Kit (JDK) installed.
   - Apache Hadoop installed and configured.

2. **Wordlists**:
   - Place the `positive.txt` and `negative.txt` files in the `input` directory.
   - Each file should contain one word per line.

3. **Environment Variables** (Optional):
   - `POSITIVE_WORDLIST_PATH`: Path to the positive wordlist file (default: `input/positive.txt`).
   - `NEGATIVE_WORDLIST_PATH`: Path to the negative wordlist file (default: `input/negative.txt`).

## Usage
1. **Compile the Code**:
   ```bash
   javac -cp $(hadoop classpath) -d . src/main/java/com/example/SentimentMapper.java
   ```

2. **Create a JAR File**:
   ```bash
   jar -cvf SentimentAnalysis.jar -C . com
   ```

3. **Run the MapReduce Job**:
   ```bash
   hadoop jar SentimentAnalysis.jar com.example.SentimentMapper <input_path> <output_path>
   ```
   Replace `<input_path>` with the path to your input text file(s) and `<output_path>` with the desired output directory.

## Output
The output will include the following metrics:
- `PositiveWordCount`: Total count of positive words.
- `NegativeWordCount`: Total count of negative words.
- `PositiveScore`: Percentage of positive words.
- `NegativeScore`: Percentage of negative words.
- `SentimentRatio`: Ratio of positive to negative sentiment.

## Error Handling
- If the wordlist files are missing or invalid, the program will throw an `IOException`.
- Ensure the input text files are properly formatted and accessible.
