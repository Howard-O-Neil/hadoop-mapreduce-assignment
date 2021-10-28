package com.hadoop_dev;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.I;

public class App {
    public static class CustsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text("same-id"), new Text("0###" + parts[0] + "###" + parts[3]));
        }
    }

    public static class TxnsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text("same-id"), new Text("1###" + parts[4] + "###" + parts[2]));
        }
    }

    public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> listStr = new ArrayList<String>();
            values.forEach(new Consumer<Text>() {
                @Override
                public void accept(Text t) {
                    listStr.add(t.toString());
                }
            });
            Collections.sort(listStr, Comparator.reverseOrder());

            HashMap<String, Float> cust = new HashMap<String, Float>();
            HashMap<String, Float[]> trans = new HashMap<String, Float[]>();

            for (int i = listStr.size() - 1; i >= 0; i--) {
                String[] parts = listStr.get(i).split("###");

                if (parts[0].equals("0")) { // cust
                    cust.put(parts[1], Float.parseFloat(parts[2]));
                    continue;
                }
                if (!trans.containsKey(parts[1])) {
                    trans.put(parts[1], new Float[4]);
                    trans.get(parts[1])[0] = 0.0f;
                    trans.get(parts[1])[1] = 0.0f;
                    trans.get(parts[1])[2] = null;
                    trans.get(parts[1])[3] = null;
                }

                // sum to find average
                trans.get(parts[1])[0] += cust.get(parts[2]).floatValue();
                trans.get(parts[1])[1] += 1.0f;

                // find min age
                if (trans.get(parts[1])[2] == null)
                    trans.get(parts[1])[2] = cust.get(parts[2]).floatValue();
                else if (cust.get(parts[2]).floatValue() < trans.get(parts[1])[2].floatValue())
                    trans.get(parts[1])[2] = cust.get(parts[2]).floatValue();

                // find max age
                if (trans.get(parts[1])[3] == null)
                    trans.get(parts[1])[3] = cust.get(parts[2]).floatValue();
                else if (cust.get(parts[2]).floatValue() > trans.get(parts[1])[3].floatValue())
                    trans.get(parts[1])[3] = cust.get(parts[2]).floatValue();
            }

            // output result

            String gameType = "";
            Float minAvg = null;

            context.write(new Text("\n====="), new Text("List Game types with age metrics"));
            for (Map.Entry<String, Float[]> entry : trans.entrySet()) {
                Float avg = entry.getValue()[0].floatValue() / entry.getValue()[1].floatValue();

                if (minAvg == null) {
                    minAvg = avg;
                    gameType = entry.getKey();
                } else if (avg < minAvg) {
                    minAvg = avg;
                    gameType = entry.getKey();
                }

                context.write(new Text(""), new Text(String.format("%s-> Min: %s, Max: %s, Avg: %s", entry.getKey(),
                        entry.getValue()[2], entry.getValue()[3], avg)));
            }

            context.write(new Text("\n====="), new Text("Game types with lowest average age"));
            context.write(new Text(""), new Text(String.format("%s-> Avg: %s", gameType, minAvg)));
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
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
