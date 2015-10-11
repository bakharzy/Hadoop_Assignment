package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class calcmin {

// The mapper is the same as example mapper
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            String[] line = value.toString().split("\\s+");
            double observation = Double.parseDouble(line[1]);
            output.collect(new Text("lines"), new DoubleWritable(observation));}
    }
//    Combiner added

    public static class Combine extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            
            double sumOfAllData = 0.0;
            double numberCounter = 0.0;
            double average = 9027230.275509289;	 //hardcoded avg for variance calculation
            double varianceSum = 0.0;
            double maxValue = Double.MIN_VALUE;
            double minValue = Double.MAX_VALUE;

            while (values.hasNext()) {		
                DoubleWritable value = values.next();
                double doubleValue = value.get();

                varianceSum = varianceSum + Math.pow((doubleValue - average), 2);
                numberCounter = numberCounter + 1;
                sumOfAllData = sumOfAllData + doubleValue;
                minValue = Math.min(minValue, doubleValue);
                maxValue = Math.max(maxValue, doubleValue);

            }
            
// Reducer should output values in order(ascending, alphabetial) 
            output.collect(new Text("maxValue"), new DoubleWritable(maxValue));
            output.collect(new Text("minValue"), new DoubleWritable(minValue));
            output.collect(new Text("numberCount"), new DoubleWritable(numberCounter));
            output.collect(new Text("sumOfAll"), new DoubleWritable(sumOfAllData));
            output.collect(new Text("varianceSum"), new DoubleWritable(varianceSum));
        }
    }

//    Reducer which works based on the keys, since each reducer is associated with one key, one of the IF statements in reducer will take care of the values
    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

            double sumOfAllData = 0.0;
            double numberCounter = 0.0;
            double varianceSum = 0.0;

            double minValue = Double.MAX_VALUE;
            double maxValue = Double.MIN_VALUE;

            String stringKey = key.toString();
//Checking if the key is one the keys bellow, then do the appropriate operation...
//    Variance Sum
            if (stringKey.equalsIgnoreCase("varianceSum")) {
                while (values.hasNext()) {
                    DoubleWritable value = values.next();
                    varianceSum = varianceSum + value.get();
                }
                output.collect(key, new DoubleWritable(varianceSum));
            }
//     min
            if (stringKey.equalsIgnoreCase("minValue")) {
                while (values.hasNext()) {
                    DoubleWritable value = values.next();
                    minValue = Math.min(minValue, value.get());
                }
                output.collect(key, new DoubleWritable(minValue));
            }
//     max
            if (stringKey.equalsIgnoreCase("maxValue")) {
                while (values.hasNext()) {
                    DoubleWritable value = values.next();
                    maxValue = Math.max(maxValue, value.get());
                }
                output.collect(key, new DoubleWritable(maxValue));
            }
//     Number count   
            if (stringKey.equalsIgnoreCase("numberCount")) {
                while (values.hasNext()) {
                    DoubleWritable value = values.next();
                    numberCounter = numberCounter + value.get();
                }
                output.collect(key, new DoubleWritable(numberCounter));
            }
//    Sum of all numbers
            if (stringKey.equalsIgnoreCase("sumOfAll")) {
                while (values.hasNext()) {
                    DoubleWritable value = values.next();
                    sumOfAllData = sumOfAllData + value.get();
                }
                output.collect(key, new DoubleWritable(sumOfAllData));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(calcmin.class);
        conf.setJobName("calcmin");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Combine.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
