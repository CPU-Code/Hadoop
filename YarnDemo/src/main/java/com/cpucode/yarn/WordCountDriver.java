package com.cpucode.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * @author : cpucode
 * @date : 2021/12/1 19:21
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class WordCountDriver {
    private static Tool tool;

    public static void main(String[] args) throws Exception{
        // 创建配置
        Configuration conf = new Configuration();

        switch (args[0]) {
            case "wordcount" :
                tool = new WordCount();
                break;
            default:
                throw new RuntimeException("no such tool " + args[0]);
        }
        // 执行程序
        int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));

        System.exit(run);
    }
}
