package com.bigData.HiveAPI;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HiveAPI
 * @Author: Jackson_J
 * @CreateTime: 2019-02-27 21:42
 * @Description: 创建自定义函数 实现字符串的拼接
 */
public class DIYHiveFun extends UDF {
    // 重写一个方法 方法名称必须是 evaluate
    public String evaluate(String ename,String sal) {
        return ename+"---"+sal;
    }
}
