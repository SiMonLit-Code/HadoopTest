package com.czz.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author czz
 * @version 1.0
 * @date 2021/4/7 11:59
 */
public class TestMapReduce {
    public static void main(String[] args) throws IOException {
        // 加载xml文件
        Configuration conf = new Configuration(true);

        JobConf jobConf = new JobConf(conf,TestMapReduce.class);

        // 必须写！！
//        job.setJarByClass(TestMapReduce.class);
        // 设置程序名称
        jobConf.setJobName("msb");

        Path infile = new Path("/data/wc/input");
        TextInputFormat.addInputPath(jobConf,infile);

        Path outfile = new Path("data/wc/output");
        // 判断路径是否存在
        if (outfile.getFileSystem(conf).exists(outfile)){
            outfile.getFileSystem(conf).delete(outfile,true);
        }
        TextOutputFormat.setOutputPath(jobConf,outfile);

        // 传入Mapper对象和reduce对象，在之后程序运行时会被被反射成对象
        jobConf.setMapperClass(MyMapper.class);
        // 设置map处理后输出对象的类型
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);
        
        jobConf.setReducerClass(MyReduce.class);
    }

}
