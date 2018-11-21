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

    //Function checks if a string is present in an array of strings
    private static Boolean isStringInArray(String item, String[] array){
        //cycle through input array
        for(int i=0;i<array.length;i++){
            //boolean if item is present
            if(item.equals(array[i]) || array[i] == null){
                return true;
            }
        }
        return false;
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

    private static String alphabetiseWord(String word){
        String[] wordArray = word.split("");
        Arrays.sort(wordArray);
        StringBuilder builder = new StringBuilder();
        for(String letter : wordArray){
            builder.append(letter);
        }
        String string = builder.toString();
        return string;
    }

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {

        private Text alphabetisedWord = new Text();
        private Text orderedWord = new Text();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //puts the entire text of the book into a string
            String line = value.toString();
            //split the string into an array of all words in the book
            String[] splitString = line.split("[^-a-zA-Z']");

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
    }

    public static class Combiner extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

        //WORKING HERE REMOVE UNIQUE
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //initate array of unique values
            String[] resultArray = {};
            //iterate through words
            for (Text value : values) {
                //boolean is string already in the resultArray
                Boolean condition = isStringInArray(value.toString(), resultArray);
                // if not in the array
                if (!condition) {
                    //push value to the array
                    resultArray = arrayPush(value.toString(), resultArray);
                    //write the key (alphabetised word) and unique word
                    context.write(key, value);
                }
            }
        }


    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, IntWritable, Text> {

        private IntWritable count = new IntWritable();
        private Text outputValue = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //create empty array of all the anagram words
            String[] anagramWords = {};

            //initate the size of the anagrams array (how many anagram words are in the .txt file)
            int wordLength = 0;
            //iterate through the array of unique words
            for (Text value : values) {
                //is the word already covered
                Boolean condition = isStringInArray(value.toString(), anagramWords);
                //if the word is unique
                if (!condition) {
                    //increase size value
                    wordLength++;
                    //push unique word to the anagram words array
                    anagramWords = arrayPush(value.toString(), anagramWords);
                }
            }

            //if the number of unique words that have anagrams has a size greater than one
            //i.e. there is at least one anagram
            if (wordLength > 1) {
                //set the count number to the number of occurances of these anagram words
                count.set(wordLength);

                //converts the anagramWords array into an array of strings
                StringBuilder builder = new StringBuilder();
                String commaSarator = "";
                for(String anagramWord : anagramWords){
                    builder.append(commaSarator);
                    builder.append(anagramWord);
                    //seperate each word with a comma apart from last word
                    commaSarator = ", ";
                }
                String anagramsString = builder.toString();
                //set the output value to the anagramsString
                outputValue.set(anagramsString);
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