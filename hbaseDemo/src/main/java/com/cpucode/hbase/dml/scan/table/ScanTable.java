package com.cpucode.hbase.dml.scan.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 扫描数据(Scan)
 *
 * @author : cpucode
 * @date : 2022/1/27 10:56
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class ScanTable {
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
        scanTable(tableName);
    }

    public static void scanTable(String tableName) throws IOException {
        // 获取表的连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建Scan对象
        Scan scan = new Scan();

        // 扫描数据
        ResultScanner scanner = table.getScanner(scan);

        // 解析results
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" +
                                Bytes.toString(CellUtil.cloneValue(cell)));

            }
        }

        // 关闭资源
        table.close();
        connection.close();
    }
}
