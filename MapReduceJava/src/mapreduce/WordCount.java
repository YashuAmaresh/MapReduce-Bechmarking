package mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		double execTimes[] = new double[5];
		for(int i = 0; i < 5; ++i) {
			Path output = new Path(files[1] + i);
			Job j = new Job(c, "wordcount");
			j.setJarByClass(WordCount.class);
			j.setMapperClass(MapForWordCount.class);
			j.setReducerClass(ReduceForWordCount.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			j.setNumReduceTasks(15);
			FileInputFormat.addInputPath(j, input);
			FileOutputFormat.setOutputPath(j, output);
			long start = System.nanoTime();
			boolean res = j.waitForCompletion(true);
			long end = System.nanoTime();
			execTimes[i] = (double)(end - start)/1000000000;
			System.out.println("Reduce Tasks: " + j.getNumReduceTasks());
			System.out.println("Time Taken: " + (end - start) + ":" + execTimes[i]);
		}
		
		double sum = 0;
		for(int i = 0; i < execTimes.length; ++i) {
			sum += execTimes[i];
		}
		System.out.println("Average time taken: " + sum / 5);
		System.exit(0);
	}

	public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			Pattern pt = Pattern.compile("[^a-zA-Z0-9]");
	        Matcher match= pt.matcher(line);
	        while(match.find())
	        {
	            String s = match.group();
	            line = line.replaceAll("\\"+s, " ");
	        }
	        
			String[] words = line.split(" ");
			for (String word : words) {
				Text outputKey = new Text(word.toUpperCase().trim());
				IntWritable outputValue = new IntWritable(1);
				con.write(outputKey, outputValue);
			}
		}
	}

	public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
}