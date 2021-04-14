package com.czz.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author czz
 * @version 1.0
 * @date 2021/4/7 13:32
 */
public class MyMapper implements Mapper<Object, Text,Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf job) {

    }

    /**
     *
     * @param key 每一行字符串自己第一字节面向源文件的偏移量
     * @param value 每一行的数据
     * @param output 输出对象
     * @param reporter 记录对象
     * @throws IOException
     */
    @Override
    public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()){
            word.set(itr.nextToken());
            output.collect(word,one);
        }
    }
}
