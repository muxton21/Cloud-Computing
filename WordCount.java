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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    private static String[] alphabetiseArray(String[] array){
        Arrays.sort(array);
        return array;
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

    /* private static String[][] arrayPushArrayOfArrays(String[] item, String[][] oldArray){
        // New array with an additional element
        String[][] newArray = new String[oldArray.length +1][];
        // Copy all the elements from the initial array to the new array
        for(int i=0; i < oldArray.length; i++){
            newArray[i] = oldArray[i];
        }

        // Assign the new element with any value
        newArray[newArray.length - 1] = item;
        // Set the new array to the initial array while disposing of the initial array
        oldArray = newArray;
        //return old array with new element pushed
        return oldArray;
    } */

    private static String alphabetiseWord(String word){
        String[] wordArray = word.split("");
        Arrays.sort(wordArray);
        String string = "";
        for(int i=0;i<wordArray.length;i++){
            string += wordArray[i];
        }
        return string;
    }

    //Function checks if a string is present in an array of strings
    private static Boolean isStringInArray(String item, String[] array){
        //cycle through input array
        for(int i=0;i<array.length;i++){
            //boolean if item is present
            Boolean stringEqual = item.equals(array[i]);
            if(stringEqual){
                return true;
            }
        }
        return false;
    }

    private static String[] checkForDuplicates(String[] array, String[] resultArray){
        //removes all duplicate words from array to leave only unique values
        for(int i=0;i<array.length;i++){
            //if value in split string is in the result array
            Boolean condition = isStringInArray(array[i], resultArray);
            if(!condition){
                resultArray = arrayPush(array[i], resultArray);
            }
        }
        return resultArray;
    }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text alphabetisedWord = new Text();
    private Text orderedWord = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

        //An array to store all the unique words in the e-book
        String[] resultArray = {};

        //puts the entire text of the book into a string
        String line = value.toString();
        //split the string into an array of all words in the book
        String[] splitString = line.split("[^a-zA-Z'\"]");

        //remove any punctuation
        for(int i=0;i<splitString.length;i++){
            splitString[i] = splitString[i].toLowerCase().replaceAll("[^a-z ]", "");
        }

        for(int i=0;i<splitString.length;i++){
            alphabetisedWord.set(alphabetiseWord(resultArray[i]));
            orderedWord.set(resultArray[i]);
            context.write(alphabetisedWord, orderedWord);
        }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String resultWord = "";
      for (Text val : values) {
        resultWord += val.get();
      }
      result.set(resultWord);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

