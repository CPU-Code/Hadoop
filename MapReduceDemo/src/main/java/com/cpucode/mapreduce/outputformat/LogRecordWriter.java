package com.cpucode.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author : cpucode
 * @date : 2021/12/2 17:21
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private  FSDataOutputStream cpucodeOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) throws IOException {
        // 创建两条流
        FileSystem fs = FileSystem.get(job.getConfiguration());

        cpucodeOut = fs.create(new Path("D:\\Date\\github\\Hadoop\\MapReduceDemo\\src\\main\\resources\\output\\8\\cpucode.log"));
        otherOut = fs.create(new Path("D:\\Date\\github\\Hadoop\\MapReduceDemo\\src\\main\\resources\\output\\8\\other.log"));
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = text.toString();

        // 具体写
        if (log.contains("cpucode")){
            cpucodeOut.writeBytes(log + "\n");
        }else {
            otherOut.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 关流
        IOUtils.closeStream(cpucodeOut);
        IOUtils.closeStream(otherOut);
    }
}
