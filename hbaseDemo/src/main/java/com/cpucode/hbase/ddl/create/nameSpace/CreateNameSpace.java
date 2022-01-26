package com.cpucode.hbase.ddl.create.nameSpace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 创建命名空间
 *
 * @author : cpucode
 * @date : 2022/1/26 21:55
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class CreateNameSpace {
    private static Connection connection ;

    static{
        //创建配置信息并配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cpucode101,cpucode102,cpucode103");
        try {
            // 获取与HBase的连接
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String ns = "cpuTest";

        createNameSpace(ns);
    }

    public static void createNameSpace(String ns) throws IOException {
        // 获取DDL操作对象
        Admin admin = connection.getAdmin();
        // 创建命名空间描述器
        NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();
        // 执行创建命名空间操作
        try {
            admin.createNamespace(build);
        } catch (NamespaceExistException e) {
            System.err.println("命名空间已存在！");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 关闭连接
        admin.close();
        connection.close();

    }
}
