package com.BigData.MapReduce.Pig.diy.function;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Pig.diy.function
 * @Author: Jackson_J
 * @CreateTime: 2019-03-03 11:24
 * @Description:  pig 自定义 过滤函数  eg 查询工资大于2000块钱的员工
 * 1. 继承 FilterFunc 父类 实现 exec方法
 */
public class FilterFun extends FilterFunc {
    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        // tuple表示调用时传递的参数
        // 在 pigLatin 语句中
        // grunt> e = filter e by jar包路径 Pig.diy.function.FilterFun(sal)
        int sal = (int) tuple.get(0); //取参数列表第一个参数
        if (sal >2000) {
            return true;
        } else {
            return  false;
        }
    }
}
