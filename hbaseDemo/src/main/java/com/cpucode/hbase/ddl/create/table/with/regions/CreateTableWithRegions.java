package com.cpucode.hbase.ddl.create.table.with.regions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 创建预分区
 * @author : cpucode
 * @date : 2022/1/27 16:05
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class CreateTableWithRegions {
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
        createTableWithRegions("", "staff5", "info");
    }

    public static void createTableWithRegions(String nameSpaceName, String tableName,String... cfs) throws IOException {
        if(existsTable(nameSpaceName, tableName)){
            System.err.println((nameSpaceName == null || nameSpaceName.equals("") ?
                    "default" :
                    nameSpaceName)  + ":" + tableName  + "表已经存在");
            return ;
        }

        if (cfs == null || cfs.length < 1){
            System.err.println("至少指定一个列族");
            return ;
        }

        // 获取DDL操作对象
        Admin admin = connection.getAdmin();

        //6.创建表描述器构造器
        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpaceName, tableName));

        //7.循环添加列族信息
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));

            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        TableDescriptor build = tableDescriptorBuilder.build();

        byte [][] splitkeys = new byte[4][];

        splitkeys[0] = Bytes.toBytes("1000");
        splitkeys[1] = Bytes.toBytes("2000");
        splitkeys[2] = Bytes.toBytes("3000");
        splitkeys[3] = Bytes.toBytes("4000");

        //8.执行创建表的操作
        admin.createTable(build, splitkeys);

        // 关闭连接
        admin.close();
    }


    /**
     * 判断表是否存在
     */
    public static boolean existsTable(String nameSpaceName, String tableName) throws IOException {
        //3.获取DDL操作对象
        Admin admin = connection.getAdmin();
        //4.判断表是否存在操作
        return admin.tableExists(TableName.valueOf(nameSpaceName, tableName)) ;
    }
}
