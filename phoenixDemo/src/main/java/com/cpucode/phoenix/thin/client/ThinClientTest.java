package com.cpucode.phoenix.thin.client;

import java.sql.*;
import org.apache.phoenix.queryserver.client.ThinClientUtil;

/**
 * JDBC编码步骤:
 *      注册驱动
 *      获取连接
 *      编写SQL
 *      预编译
 *      设置参数
 *      执行SQL
 *      封装结果
 *      关闭连接
 *
 * @author : cpucode
 * @date : 2022/1/28 16:40
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class ThinClientTest {
    public static void main(String[] args) throws SQLException {
        //1. 获取连接
        String connectionUrl = ThinClientUtil.getConnectionUrl("cpucode101", 8765);

        Connection connection = DriverManager.getConnection(connectionUrl);

        //2. 编写SQL
        String sql = "select * from \"person1\"";

        //3. 预编译
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //4. 执行sql
        ResultSet resultSet = preparedStatement.executeQuery();

        //5. 封装结果
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "\t" +
                    resultSet.getString(2));
        }

        //6. 关闭连接
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
