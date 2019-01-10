package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.number;

import org.apache.hadoop.io.LongWritable;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.number
 * @Author: 15568
 * @CreateTime: 2019-01-02 21:35
 * @Description: MR 数字类型比较器 默认升序 这里改为降序
 */
public class MyNumberComparator extends LongWritable.Comparator {
    /**
     * 定义自己的排序规则  改为降序
     * @param b1
     * @param s1
     * @param l1
     * @param b2
     * @param s2
     * @param l2
     * @return
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        // 降序
        return -super.compare(b1, s1, l1, b2, s2, l2);
    }
}
