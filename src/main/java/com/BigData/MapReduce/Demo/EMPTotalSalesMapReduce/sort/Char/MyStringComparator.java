package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Char;

import org.apache.hadoop.io.Text;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Char
 * @Author: 15568
 * @CreateTime: 2019-01-02 21:54
 * @Description: 自定义字符串排序规则 默认是字典排序
 */
public class MyStringComparator extends Text.Comparator {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -super.compare(b1, s1, l1, b2, s2, l2);
    }
}
