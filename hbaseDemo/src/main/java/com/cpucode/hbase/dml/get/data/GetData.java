package com.cpucode.hbase.dml.get.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 单条数据查询(GET)
 *
 * @author : cpucode
 * @date : 2022/1/27 10:46
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class GetData {
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

        getDate(tableName, "1001", "info1", "value");
    }

    public static void getDate(String tableName, String rowKey, String cf, String cn) throws IOException {
        // 获取表的连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 指定列族查询
        // get.addFamily(Bytes.toBytes(cf));
        // 指定列族:列查询
        // get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));


        //查询数据
        Result result = table.get(get);

        // 解析result
        for (Cell cell : result.rawCells()) {
            System.out.println("ROW:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    " CF:" + Bytes.toString(CellUtil.cloneFamily(cell))+
                    " CL:" + Bytes.toString(CellUtil.cloneQualifier(cell))+
                    " VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        // 关闭连接
        table.close();
        connection.close();
    }
}
