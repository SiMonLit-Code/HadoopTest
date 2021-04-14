package com.czz.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author czz
 * @version 1.0
 * @date 2021/4/7 14:02
 */
public class MyReduce implements Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable result = new IntWritable();

    /**
     * 相同的key为一组，一组调用一次reduce
     *  例如：
     *      hello 1
     *      hello 1
     *      hello 1
     *      ...
     * @param key
     * @param values
     * @param output
     * @param reporter
     * @throws IOException
     */
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            IntWritable val = values.next();
            sum += val.get();
        }
        result.set(sum);
        output.collect(key, result);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf job) {

    }
}
