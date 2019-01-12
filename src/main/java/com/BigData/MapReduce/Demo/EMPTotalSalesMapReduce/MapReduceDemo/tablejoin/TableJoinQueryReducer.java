package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.tablejoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.tablejoin
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 11:27
 * @Description:   Mapper 的特性  具有相同key 的数据会一起出来 即  reduce() 方法处理的都是 相同key2 的数据
 *   多表等值连接 Reducer 类
 */
public class TableJoinQueryReducer extends Reducer<LongWritable, Text,Text,Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          // 保存 部门名称 和员工名称
        String deptName = "";
        List<String> empNames= new ArrayList<>();
        for (Text name :values) {
            String val = name.toString();
            if (StringUtils.startsWith(val,"*")) { //部门名称
                 deptName = val.substring(1);
            } else { //员工名称
                empNames.add(val);
            }
        }
        // 输出  部门号   员工名称集合
        context.write(new Text(deptName),new Text(StringUtils.join(empNames,",")));
    }
}
