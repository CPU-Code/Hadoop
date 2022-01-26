package com.cpucode.hbase.ddl.drop.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 删除表
 *
 * @author : cpucode
 * @date : 2022/1/26 21:33
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class DropTable {
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
        String tableName = "test";

        dropTable(tableName);
    }

    public static void dropTable(String tableName) throws IOException {
        //1.判断表是否存在
        if (!isTableExist(tableName)){
            System.err.println("需要删除的表不存在！");
            return;
        }

        //4.获取DDL操作对象
        Admin admin = connection.getAdmin();

        //5.使表下线
        TableName tableName1 = TableName.valueOf(tableName);
        admin.disableTable(tableName1);

        //6.执行删除表操作
        admin.deleteTable(tableName1);

        //7.关闭资源
        admin.close();
        connection.close();
    }

    public static boolean isTableExist(String tableName) throws IOException {

        //3.获取DDL操作对象
        Admin admin = connection.getAdmin();

        //4.判断表是否存在操作
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        //5.关闭连接
        admin.close();

        //6.返回结果
        return b;
    }
}
