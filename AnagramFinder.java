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

        private Text sortedText = new Text();
        private Text outputValue = new Text();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(),
                    " \t\n\r\f,.:()!?", false);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().trim().toLowerCase();
                sortedText.set(sort(token));
                outputValue.set(token);
                context.write(sortedText, outputValue);
            }
        }

        protected String sort(String input) {
            char[] cs = input.toCharArray();
            Arrays.sort(cs);
            return new String(cs);
        }

    }

    public static class Combiner extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Text> uniques = new HashSet<Text>();
            for (Text value : values) {
                if (uniques.add(value)) {
                    context.write(key, value);
                }
            }
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

        Job job = new Job(conf, "Anagram Finder");

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