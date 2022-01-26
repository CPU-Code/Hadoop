package com.cpucode.hbase.ddl.create.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 创建表
 *
 * @author : cpucode
 * @date : 2022/1/26 20:51
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class CreateTable {
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

        createTable(tableName, "info1", "info2");
    }

    public static void createTable(String tableName, String... cfs) throws IOException {
        //1.判断是否存在列族信息
        if (cfs.length <= 0){
            System.err.println("请设置列族信息！");
            return;
        }

        //2.判断表是否存在
        if (isTableExist(tableName)){
            System.err.println("需要创建的表已存在！");
            return;
        }

        //5.获取DDL操作对象
        Admin admin = connection.getAdmin();

        //6.创建表描述器构造器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

        //7.循环添加列族信息
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));

            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        //8.执行创建表的操作
        admin.createTable(tableDescriptorBuilder.build());

        //9.关闭资源
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
