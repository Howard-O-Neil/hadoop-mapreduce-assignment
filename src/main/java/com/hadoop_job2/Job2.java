package com.hadoop_job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2 {
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
        job.setJarByClass(Job2.class);
        job.setReducerClass(GametypeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GametypeMapper.class);
        Path outputPath = new Path(args[1], "job2");

        FileOutputFormat.setOutputPath(job, outputPath);
        // outputPath_1.getFileSystem(conf).delete(outputPath_1, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
