package com.cpucode.mapreduce.outputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author : cpucode
 * @date : 2021/12/2 17:20
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置自定义的 outputformat
        job.setOutputFormatClass(LogOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\Date\\github\\Hadoop\\MapReduceDemo\\src\\main\\resources\\input\\outputformat"));

        //虽然我们自定义了outputformat，但是因为我们的 outputformat 继承自 fileoutputformat
        //而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("D:\\Date\\github\\Hadoop\\MapReduceDemo\\src\\main\\resources\\output\\8"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
