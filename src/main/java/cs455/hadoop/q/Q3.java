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

public class Q3 {

    public static class Q3Mapper extends Mapper<Object, Text, Text, Text> {

        private TreeMap<String, String> map = new TreeMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (value == null || value.toString().trim().isEmpty())
                return;

            String[] csv = splitCSV(value.toString().trim());

            try {

                String year = csv[Constants.DATE_GMT].split("-")[0];
                String time = csv[Constants.TIME_GMT];
                double sample = Double.parseDouble(csv[Constants.SAMPLE_MEASUREMENT]);

                if (Integer.parseInt(year) < 2000)
                    return;

                String newKey = year +"\t"+ time;

                String val = map.get(newKey);
                if (val == null) {
                    map.put(newKey, sample +"\t1");
                } else {
                    String[] data = val.split("\t");
                    double sum = sample + Double.parseDouble(data[0]);
                    long count = 1 + Long.parseLong(data[1]);
                    map.put(newKey, sum +"\t"+ count);
                }

            } catch (Exception e) { }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }
        }

    }

    public static class Q3Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private TreeMap<String, String> map = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double sum = 0.0;
            long count = 0;

            String val = map.get(key.toString());

            if (val != null) {
                String[] data = val.split("\t");
                sum = Double.parseDouble(data[0]);
                count = Long.parseLong(data[1]);
            }

            for (Text t : values) {
                String[] data = t.toString().split("\t");
                sum += Double.parseDouble(data[0]);
                count += Long.parseLong(data[1]);
            }

            map.put(key.toString(), sum +"\t"+ count);

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String[] data = entry.getValue().split("\t");
                double val = Double.parseDouble(data[0]) / Double.parseDouble(data[1]);
                context.write(new Text(entry.getKey()), new DoubleWritable(val));
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

        System.out.println("*************************** Q3 ***************************");

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Q3");
            job.setNumReduceTasks(1);
            job.setJarByClass(Q3.class);
            job.setMapperClass(Q3Mapper.class);
            job.setReducerClass(Q3Reducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
