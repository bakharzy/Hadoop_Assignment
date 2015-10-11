package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class calcmin {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            String[] line = value.toString().split("\\s+");
            double observation = Double.parseDouble(line[1]);
            String valueConvertedToStr = "";
            double one = 1;
            int myInt = 0;
            double startRange = 4357677; //These are the start and end range values I used in my last iteration for finding quartile.
            double endRange = 4357678;
            
//            values less than startRange are grouped with "lessThanRange" key and value equal to 1=one
            if (observation < startRange) { 
                output.collect(new Text("lessThanRange"), new DoubleWritable(one));
            }
            
            if (observation >= startRange && observation < endRange) {
                myInt = (int) observation; // I used this for not considering the digits after decimal point
                valueConvertedToStr = String.valueOf(observation);
                output.collect(new Text(valueConvertedToStr), new DoubleWritable(one));
//                output.collect(new Text("betweenRange"), new DoubleWritable(one)); //can be used when there are many number in the range than we need their count
//                output.collect(new Text(obs.substring(0, 2)), new DoubleWritable(one)); // can be used to group values based on their first 2 digits and count their members
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

            double count = 0.0;
            while (values.hasNext()) {
                DoubleWritable value = values.next();
                double doubleValue = value.get();
                count = count + doubleValue;
            }
            output.collect(key, new DoubleWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(calcmin.class);
        conf.setJobName("calcmin");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
