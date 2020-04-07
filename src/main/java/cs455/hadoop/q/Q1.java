package cs455.hadoop.q;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class Q1 {

	public static class Q1Mapper extends Mapper<Object, Text, Text, NullWritable> {

		private TreeMap<String, Boolean> map = new TreeMap<>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			if (value == null || value.toString().trim().isEmpty())
				return;

			String[] data = splitCSV(value.toString().trim());

			try {
				int state = Integer.parseInt(data[Constants.STATE_CODE]);
				int county = Integer.parseInt(data[Constants.COUNTY_CODE]);
				int site = Integer.parseInt(data[Constants.SITE_NUM]);
				map.put(state +"\t"+ county +"\t"+ site, true);
			} catch (Exception e) { }

		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			for (String key : map.keySet()) {
				context.write(new Text(key), NullWritable.get());
			}
		}

	}

	public static class Q1Reducer extends Reducer<Text, NullWritable, Text, IntWritable> {

		private TreeMap<String, Boolean> siteMap = new TreeMap<>();
		private TreeMap<String, Integer> stateMap = new TreeMap<>();

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			siteMap.put(key.toString(), true);
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

			for (String key : siteMap.keySet()) {
				String stateCode = key.split("\t")[0];
				Integer stateVal = stateMap.get(stateCode);
				if (stateVal == null)
					stateMap.put(stateCode, 1);
				else
					stateMap.put(stateCode, stateVal+1);
			}

			for (Map.Entry<String, Integer> entry : sortedSet(stateMap)) {
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}

	}

	private static String[] splitCSV(String line) {

		// Source: https://mkyong.com/java/how-to-read-and-parse-csv-file-in-java/
		ArrayList<String> splits = new ArrayList<>();
		StringBuffer buf = new StringBuffer();
		boolean inQuotes = false;
		
		for (char c : line.toCharArray()) {
			if (inQuotes) {
				if (c == '\"')
					inQuotes = false;
				else
					buf.append(c);
			} else {
				if (c == ',') {
					splits.add(buf.toString());
					buf = new StringBuffer();
				} else if (c == '\"') {
					inQuotes = true;
				} else
					buf.append(c);
			}
		}
		
		splits.add(buf.toString());
		
		return splits.toArray(new String[0]);
	}

	private static <K,V extends Comparable<? super V>> SortedSet<Map.Entry<K,V>> sortedSet(Map<K,V> map) {
		// Source: https://stackoverflow.com/questions/2864840/treemap-sort-by-value

		SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(new Comparator<Map.Entry<K,V>>() {
			@Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
				int res = -e1.getValue().compareTo(e2.getValue());
				return res != 0 ? res : 1;
			}
		});

		sortedEntries.addAll(map.entrySet());

		return sortedEntries;
	}

	public static void main(String[] args) {

		System.out.println("*************************** Q1 ***************************");

		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Q1");
			job.setNumReduceTasks(1);
			job.setJarByClass(Q1.class);
			job.setMapperClass(Q1Mapper.class);
			job.setReducerClass(Q1Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Q1Mapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Q1Mapper.class);
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
