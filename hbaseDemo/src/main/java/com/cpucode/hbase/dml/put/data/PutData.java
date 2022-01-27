package com.cpucode.hbase.dml.put.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 插入数据
 *
 * @author : cpucode
 * @date : 2022/1/26 22:24
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class PutData {
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
        putData(tableName, "1001", "info1", "name", "cpuCode");
    }

    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        //3.获取表的连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //4.创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //5.放入数据
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        //6.执行插入数据操作
        table.put(put);


        //7.关闭连接
        table.close();
        connection.close();
    }

}
