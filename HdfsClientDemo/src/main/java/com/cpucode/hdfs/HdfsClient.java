package com.cpucode.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * HDFS  zookeeper
 *
 * @author : cpucode
 * @date : 2021/11/18 13:57
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class HdfsClient {

    private FileSystem fs;

    /**
     * 创建目录
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // FileSystem fs = FileSystem.get(newURI("hdfs://cpucode100:8020"), configuration);

        FileSystem fs = FileSystem.get(new URI("hdfs://cpucode100:8020"), configuration, "root");
        // 2 创建目录
        fs.mkdirs(new Path("/cpu/test/"));

        // 3 关闭资源
        fs.close();
    }


    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接的集群nn地址
        URI uri = new URI("hdfs://cpucode100:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication", "2");
        // 用户
        String user = "root";

        // 1 获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 3 关闭资源
        fs.close();
    }

    /**
     * 优化创建目录
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testMkdir() throws IOException {
        // 2 创建一个文件夹
        fs.mkdirs(new Path("/test/code"));
    }

    /**
     * 上传
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        /**
         * delSrc : 表示删除原数据
         * overwrite : 是否允许覆盖
         * src : 原数据路径
         * dst : 目的地路径
         */
        fs.copyFromLocalFile(false,
                true,
                new Path("D:\\Date\\github\\Hadoop\\HdfsClientDemo\\src\\main\\resources\\test100.txt"),
                new Path("hdfs://cpucode100/test/"));
    }

    /**
     * 文件下载
     * @throws IOException
     */
    @Test
    public void testGet() throws IOException {
        /**
         * delSrc : 原文件是否删除
         * src : 原文件路径HDFS
         * dst : 目标地址路径Win
         * useRawLocalFileSystem : 是否校验
         */
        fs.copyToLocalFile(false,
                new Path("hdfs://cpucode100/test/"),
                new Path("D:\\Date\\github\\Hadoop\\HdfsClientDemo\\src\\main\\resources\\"),
                false);
    }

    /**
     * 删除
     */
    @Test
    public void testRm() throws IOException {
        /**
         * 删除文件
         * var1 : 要删除的路径
         * var2 : 是否递归删除
         */
       // fs.delete(new Path("/"), false);

        // 删除空目录
       // fs.delete(new Path("/cpu"), false);


        // 删除非空目录
       fs.delete(new Path("/cpucode"), true);
    }


    public void testMv(){

    }

}
