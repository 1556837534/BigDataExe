package com.BigData.MapReduce.Pig.diy.function;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Pig.diy.function
 * @Author: Jackson_J
 * @CreateTime: 2019-03-03 11:29
 * @Description: pig 自定义运算函数 eg 根据员工的薪水来判断薪水的级别
 * 1. 继承 EvalFunc 泛型T 表示运算返回的类型    打成jar包
 */
public class RuntionFun extends EvalFunc<String> {
    @Override
    public String exec(Tuple tuple) throws IOException {
        // tuple表示调用时传递的参数
        // 在 pigLatin 语句中
        // grunt> e = foreach e generate jar包路径 Pig.diy.function.RuntionFun(sal)
        // 取出 薪水
        int sal = (int)tuple.get(0);
        if (sal <1000 ) {
            return "Grade A";
        } else if (sal >=1000 && sal <= 3000) {
            return "Grade B";
        } else {
            return "Grade C";
        }
    }
}
