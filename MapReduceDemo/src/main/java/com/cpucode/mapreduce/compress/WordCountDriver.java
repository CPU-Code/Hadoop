package com.cpucode.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author : cpucode
 * @date : 2021/12/7 14:01
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取job
        Configuration conf = new Configuration();

        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);

        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);

        // 2 设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出的kV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\Date\\github\\Hadoop\\MapReduceDemo\\src\\main\\resources\\input\\compress"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Date\\github\\Hadoop\\MapReduceDemo\\src\\main\\resources\\output\\13"));

        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
//      FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
