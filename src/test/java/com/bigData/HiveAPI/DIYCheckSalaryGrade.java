package com.bigData.HiveAPI;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HiveAPI
 * @Author: Jackson_J
 * @CreateTime: 2019-02-27 21:46
 * @Description: 判断员工薪水级别
 *    可以将其打成一个Jar 包 加入到Hive 的classPath 路径中
 *    hive> add jar jar路径（Linux 路径）
 *    创建一个别名(临时函数) 来代办自定义函数
 *    hive > create temporary function myconcat as 'demo.udf.MyConcatString';
 *    hive > create temporary function checksalary as 'demo.udf.CheckSalaryGrade';
 */
public class DIYCheckSalaryGrade extends UDF {
    public String evaluate(String salary) {
        int sal = Integer.valueOf(salary);
        if (sal < 1000) {
            return "Grade A";
        } else if (sal >= 1000 && sal <=3000) {
            return  "Grade B";
        } else {
            return "Grade C";
        }
    }
}
