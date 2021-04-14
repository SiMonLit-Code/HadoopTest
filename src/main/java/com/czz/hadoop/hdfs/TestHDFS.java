package com.czz.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author czz
 * @version 1.0
 * @date 2021/3/30 11:14
 */
public class TestHDFS {
    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws IOException {
        // 传入true加载配置文件，false不加载
        // 自动读取环境变量HADOOP_USER_NAME
        conf = new Configuration(true);
        fs = FileSystem.get(conf);
    }
    @Test
    public void mkdir() throws IOException {
        Path dir = new Path("/czz");
        if (fs.exists(dir)){
            fs.delete(dir,true);
        }
        fs.mkdirs(dir);
    }
//    @Test
    public void upload() throws IOException {
        BufferedInputStream input = new BufferedInputStream(new FileInputStream("./data/hello.txt"));
        Path outfile = new Path("/czz/out.txt");
        FSDataOutputStream output = fs.create(outfile);

        IOUtils.copyBytes(input,output,conf,true);
    }
    @Test
    public void blocks() throws IOException {
        // 从dfs中获取文件元数据
        Path files = new Path("/user/root/data.txt");
        FileStatus status = fs.getFileStatus(files);
        // 获取文件长度从0字节到最长字节的长度
        // 获取所有块
        BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());
        // 遍历所有块
        for (BlockLocation block : blocks) {
            System.out.println(block);
        }
    }
    @After
    public void close() throws IOException {
        fs.close();
    }
}
