package com.cpucode.phoenix.thick.client;

import java.sql.*;
import java.util.Properties;

/**
 * @author : cpucode
 * @date : 2022/1/28 17:00
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class ThickClientTest {
    public static void main(String[] args) throws SQLException {
        //1. 获取连接
        String url = "jdbc:phoenix:cpucode101,cpucode102,cpucode103:2181";
        Properties properties = new Properties();
        properties.put("phoenix.schema.isNamespaceMappingEnabled", "true");

        Connection connection = DriverManager.getConnection(url, properties);

        //2. 编写SQL
        String sql = "select * from \"person1\"";

        //3. 预编译
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //4. 执行sql
        ResultSet resultSet = preparedStatement.executeQuery();

        //5. 封装结果
        while(resultSet.next()){
            String line = resultSet.getString("id") + " : " +
                    resultSet.getString("salary") ;

            System.out.println(line);
        }

        //6. 关闭连接
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
