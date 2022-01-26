package com.cpucode.hbase.table.exist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * 判断表是否存在
 * @author : cpucode
 * @date : 2022/1/26 17:26
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class IsTableExist {

    private static Connection connection ;

    static{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cpucode101,cpucode102,cpucode103");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String tableName = "test";

        boolean tableExist = isTableExist(tableName);
        System.out.println(tableExist);
    }


    public static boolean isTableExist(String tableName) throws IOException {

        //3.获取DDL操作对象
        Admin admin = connection.getAdmin();

        //4.判断表是否存在操作
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        //5.关闭连接
        admin.close();
        connection.close();

        //6.返回结果
        return b;
    }
}
