package com.bigData.HiveAPI;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HiveAPI
 * @Author: Jackson_J
 * @CreateTime: 2019-02-27 21:06
 * @Description: ${Description}
 */
public class TestHive {
    @Test
    public void test () {
         String sql = "select * from emp";
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = JDBCUtils.getConnection();
            if (connection != null) {
                //得到 sql 运行环境
                statement = connection.createStatement();
                resultSet = statement.executeQuery(sql);
                while (resultSet.next()) {
                    // 取出姓名与薪水
                    String ename = resultSet.getString("ename");
                    Double sal = resultSet.getDouble("sal");
                    System.out.println("名称:"+ename+"---薪水:"+sal);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.releas(connection,statement,resultSet);

        }
    }

    public void testDIYConcatString() {

    }
}
