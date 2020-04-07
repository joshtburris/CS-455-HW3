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

public class Q2 {

    public static class Q2Mapper extends Mapper<Object, Text, Text, Text> {

        double eastSum = 0.0, westSum = 0.0;
        long eastCount = 0, westCount = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (value == null || value.toString().trim().isEmpty())
                return;

            String[] data = splitCSV(value.toString().trim());

            try {

                String state = data[Constants.STATE_NAME];
                double sample = Double.parseDouble(data[Constants.SAMPLE_MEASUREMENT]);

                int coast = getCoast(state);
                if (coast == 1) {
                    eastSum += sample;
                    ++eastCount;
                } else if (coast == -1) {
                    westSum += sample;
                    ++westCount;
                }

            } catch (Exception e) { }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("e"), new Text(eastSum +"\t"+ eastCount));
            context.write(new Text("w"), new Text(westSum +"\t"+ westCount));
        }

    }

    public static class Q2Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

        double eastSum = 0.0, westSum = 0.0;
        double eastCount = 0.0, westCount = 0.0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            boolean isEast = key.toString().compareTo("e") == 0;

            for (Text val : values) {
                String[] data = val.toString().split("\t");
                if (isEast) {
                    eastSum += Double.parseDouble(data[0]);
                    eastCount += Double.parseDouble(data[1]);
                } else {
                    westSum += Double.parseDouble(data[0]);
                    westCount += Double.parseDouble(data[1]);
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("East Coast mean SO2 levels:"), new DoubleWritable(eastSum / eastCount));
            context.write(new Text("West Coast mean SO2 levels:"), new DoubleWritable(westSum / westCount));
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

    // Returns 1 for east coast, -1 for west coast, and 0 for neither.
    private static int getCoast(String state) {
        switch (state) {
            case "California": case "Oregon": case "Washington": case "Alaska":
                return -1;

            case "Maine":case "New Hampshire": case "Massachusetts" : case "Rhode Island": case "Connecticut":
            case "New York": case "New Jersey": case "Delaware": case "Maryland": case "Virginia":
            case "North Carolina": case "South Carolina": case "Georgia": case "Florida": case "Pennsylvania":
            case "Washington, DC":
                return 1;
        }
        return 0;
    }

    public static void main(String[] args) {

        System.out.println("*************************** Q2 ***************************");

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Q2");
            job.setNumReduceTasks(1);
            job.setJarByClass(Q2.class);
            job.setMapperClass(Q2Mapper.class);
            job.setReducerClass(Q2Reducer.class);
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
