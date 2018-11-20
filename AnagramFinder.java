import java.io.*;
import java.net.URL;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AnagramFinder {

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {

        private Text alphabetisedWord = new Text();
        private Text orderedWord = new Text();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //puts the entire text of the book into a string
            String line = value.toString();
            //split the string into an array of all words in the book
            String[] splitString = line.split("[^a-zA-Z'\"]");

            //remove any punctuation
            for(int i=0;i<splitString.length;i++){
                splitString[i] = splitString[i].toLowerCase().replaceAll("[^a-z ]", "");
            }

            //set each word to the key value pair in the mapper
            for(int i=0;i<splitString.length;i++){
                alphabetisedWord.set(alphabetiseWord(splitString[i]));
                orderedWord.set(splitString[i]);
                context.write(alphabetisedWord, orderedWord);
            }
        }

        private static String alphabetiseWord(String word){
            String[] wordArray = word.split("");
            Arrays.sort(wordArray);
            String string = "";
            for(int i=0;i<wordArray.length;i++){
                string += wordArray[i];
            }
            return string;
        }

    }

    public static class Combiner extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

        //WORKING HERE REMOVE UNIQUE
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] resultArray = {};
            for (Text value : values) {
                Boolean condition = isStringInArray(value.toString(), resultArray);
                if (!condition) {
                    resultArray = arrayPush(value.toString(), resultArray);
                    context.write(key, value);
                }
            }
        }

        private static String[] arrayPush(String item, String[] oldArray){
            // New array with an additional element
            String[] newArray = new String[oldArray.length + 1];
            // Copy all the elements from the initial array
            for(int k = 0; k < oldArray.length; k++){
                newArray[k] = oldArray[k];
            }

            // Assign the new element with any value
            newArray[newArray.length - 1] = item;
            // Set the new array to the initial array while disposing of the inital array
            oldArray = newArray;
            return oldArray;
        }

    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, IntWritable, Text> {

        private IntWritable count = new IntWritable();
        private Text outputValue = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Text> uniques = new HashSet<Text>();
            int size = 0;
            StringBuilder builder = new StringBuilder();
            for (Text value : values) {
                if (uniques.add(value)) {
                    size++;
                    builder.append(value.toString());
                    builder.append(",");
                }
            }
            builder.setLength(builder.length()-1);

            if (size > 1) {
                count.set(size);
                outputValue.set(builder.toString());
                context.write(count, outputValue);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Anagram Finder");

        job.setJarByClass(AnagramFinder.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}