package com.hadoop_dev;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import java.util.function.Consumer;
import java.util.Collections;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
    public static class CustsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[0]), new Text("0###" + parts[3]));
        }
    }

    public static class TxnsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[2]), new Text("1###" + parts[4]));
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> gametypes = new ArrayList<String>();
            Float ages = null;
            for (Text t : values) {
                String[] parts = t.toString().split("###");

                if (parts[0].equals("0"))
                    ages = Float.parseFloat(parts[1]);
                else
                    gametypes.add(parts[1]);
            }

            // NullWritable wr = NullWritable.get();
            for (String s : gametypes) {
                context.write(new Text(s + "###" + String.valueOf(ages)), new Text(""));
            }
        }
    }

    public static class GametypeMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split("###");
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }

    public static class GametypeReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Float> ages = new ArrayList<Float>();
            values.forEach(new Consumer<Text>() {
                @Override
                public void accept(Text t) {
                    ages.add(Float.parseFloat(t.toString()));
                }
            });

            String res = String.format("Min: %s, Max: %s, Avg: %s", Collections.min(ages), Collections.max(ages),
                    ages.stream().mapToDouble(d -> d).average().getAsDouble());
            context.write(key, new Text(res));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Reduce-side join");
        job.setJarByClass(App.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TxnsMapper.class);
        Path outputPath = new Path(args[2], "job1");

        FileOutputFormat.setOutputPath(job, outputPath);
        // outputPath_1.getFileSystem(conf).delete(outputPath_1, true);

        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job1 = Job.getInstance(conf2, "Reduce join job 2");
        
        job1.setJarByClass(App.class);
        job1.setReducerClass(GametypeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[2], "job1"), TextInputFormat.class, GametypeMapper.class);
        Path outputPath_2 = new Path(args[2], "job2");

        FileOutputFormat.setOutputPath(job1, outputPath_2);

        job1.waitForCompletion(true);
        
        System.exit(0);
    }
}
