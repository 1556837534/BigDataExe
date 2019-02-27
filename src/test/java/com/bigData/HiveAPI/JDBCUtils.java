package com.bigData.HiveAPI;

import java.sql.*;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HiveAPI
 * @Author: Jackson_J
 * @CreateTime: 2019-02-27 20:53
 * @Description: Hive 连接工具类
 */
public class JDBCUtils {
    // Hive 的驱动 需要将官网文档中的hadoop去掉
    //private static String DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static String DRIVER = "org.apache.hive.jdbc.HiveDriver";

    //Hive 的URL  IP:Port/数据库名称(就是default 默认)
    private static String URL = "jdbc:hive2://192.168.199.135:10000/default";

    // 注册Hive 的驱动
    static {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new ExceptionInInitializerError(e);
        }
    }

    // 获取Hive 的连接
    public static Connection getConnection () {
        try {// 当前执行linux用户
            return DriverManager.getConnection(URL,"root","xyorx1ys10");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //释放连接
    public static void releas(Connection connection, Statement statement, ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                resultSet = null;
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                statement = null;
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                connection = null;
            }
        }
    }
}
