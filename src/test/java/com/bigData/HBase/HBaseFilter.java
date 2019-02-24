package com.bigData.HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HBase
 * @Author: Jackson_J
 * @CreateTime: 2019-02-23 22:35
 * @Description: HBase Filter
 */
public class HBaseFilter {

    private Configuration conf;

    @Before
    public void before() {
        //指定的配置信息: ZooKeeper
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.199.135");
    }

    @Test
    public void testCreateTable() throws Exception{

        //创建一个HBase客户端: HBaseAdmin
        HBaseAdmin admin = new HBaseAdmin(conf);

        //创建一个表的描述符: 表名
        HTableDescriptor hd = new HTableDescriptor(TableName.valueOf("emp"));

        //创建列族描述符
        HColumnDescriptor hcd1 = new HColumnDescriptor("empinfo");

        //加入列族
        hd.addFamily(hcd1);

        //创建表
        admin.createTable(hd);

        //关闭客户端
        admin.close();
    }

    // 初始化数据
    @Test
    public void testPutData() throws Exception{
        //客户端
        HTable table = new HTable(conf, "emp");

        //第一条数据
        Put put1 = new Put(Bytes.toBytes("7369"));
        put1.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("SMITH"));
        Put put2 = new Put(Bytes.toBytes("7369"));
        put2.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("800"));

        //第二条数据
        Put put3 = new Put(Bytes.toBytes("7499"));
        put3.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("ALLEN"));
        Put put4 = new Put(Bytes.toBytes("7499"));
        put4.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("1600"));

        //第三条数据
        Put put5 = new Put(Bytes.toBytes("7521"));
        put5.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("WARD"));
        Put put6 = new Put(Bytes.toBytes("7521"));
        put6.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("1250"));

        //第四条数据
        Put put7 = new Put(Bytes.toBytes("7566"));
        put7.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("JONES"));
        Put put8 = new Put(Bytes.toBytes("7566"));
        put8.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("2975"));

        //第五条数据
        Put put9 = new Put(Bytes.toBytes("7654"));
        put9.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("MARTIN"));
        Put put10 = new Put(Bytes.toBytes("7654"));
        put10.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("1250"));

        //第六条数据
        Put put11 = new Put(Bytes.toBytes("7698"));
        put11.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("BLAKE"));
        Put put12 = new Put(Bytes.toBytes("7698"));
        put12.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("2850"));

        //第七条数据
        Put put13 = new Put(Bytes.toBytes("7782"));
        put13.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("CLARK"));
        Put put14 = new Put(Bytes.toBytes("7782"));
        put14.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("2450"));

        //第八条数据
        Put put15 = new Put(Bytes.toBytes("7788"));
        put15.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("SCOTT"));
        Put put16 = new Put(Bytes.toBytes("7788"));
        put16.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("3000"));

        //第九条数据
        Put put17 = new Put(Bytes.toBytes("7839"));
        put17.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("KING"));
        Put put18 = new Put(Bytes.toBytes("7839"));
        put18.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("5000"));

        //第十条数据
        Put put19 = new Put(Bytes.toBytes("7844"));
        put19.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("TURNER"));
        Put put20 = new Put(Bytes.toBytes("7844"));
        put20.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("1500"));

        //第十一条数据
        Put put21 = new Put(Bytes.toBytes("7876"));
        put21.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("ADAMS"));
        Put put22 = new Put(Bytes.toBytes("7876"));
        put22.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("1100"));

        //第十二条数据
        Put put23 = new Put(Bytes.toBytes("7900"));
        put23.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("JAMES"));
        Put put24 = new Put(Bytes.toBytes("7900"));
        put24.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("950"));

        //第十三条数据
        Put put25 = new Put(Bytes.toBytes("7902"));
        put25.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("FORD"));
        Put put26 = new Put(Bytes.toBytes("7902"));
        put26.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("3000"));

        //第十四条数据
        Put put27 = new Put(Bytes.toBytes("7934"));
        put27.add(Bytes.toBytes("empinfo"), Bytes.toBytes("ename"), Bytes.toBytes("MILLER"));
        Put put28 = new Put(Bytes.toBytes("7934"));
        put28.add(Bytes.toBytes("empinfo"), Bytes.toBytes("sal"), Bytes.toBytes("1300"));

        //构造List
        List<Put> list = new ArrayList<>();
        list.add(put1);
        list.add(put2);
        list.add(put3);
        list.add(put4);
        list.add(put5);
        list.add(put6);
        list.add(put7);
        list.add(put8);
        list.add(put9);
        list.add(put10);
        list.add(put11);
        list.add(put12);
        list.add(put13);
        list.add(put14);
        list.add(put15);
        list.add(put16);
        list.add(put17);
        list.add(put18);
        list.add(put19);
        list.add(put20);
        list.add(put21);
        list.add(put22);
        list.add(put23);
        list.add(put24);
        list.add(put25);
        list.add(put26);
        list.add(put27);
        list.add(put28);

        //插入数据
        table.put(list);
        table.close();
    }

    /* *
	  HBase的过滤器的类型
		类似：

		//scan.addColumn(family, qualifier);
		//scan.addFamily(family)

		（*）列值过滤器: SingleColumnValueFilter
				查询工资等于3000的员工姓名
				select ename from emp where sal=3000;

		（*）列名前缀过滤器: ColumnPrefixFilter
		        查询所有员工的姓名
				select ename from emp;


		（*）多个列名前缀过滤器:MultipleColumnPrefixFilter
				查询所有员工的姓名和薪水
				select ename ,sal from emp;

		（*）行键过滤器：根据rowkey进行查询

		（*）组合多个过滤器: 查询工资等于3000的员工姓名
			1、第一个过滤器：列值过滤器
			2、第二个过滤器：列名前缀过滤器
     **/

    /**
     * 		（*）列值过滤器: SingleColumnValueFilter
     * 				查询工资等于3000的员工姓名
     * 				select ename from emp where sal=3000;
     **/
    @Test
    public void testFilter1 () throws Exception{
        //客户端
        HTable table = new HTable(conf, "emp");

        // 定义 列值过滤器: SingleColumnValueFilter
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("empinfo"), //family 列族
                Bytes.toBytes("sal"), //列名
                CompareOp.EQUAL, //比较运算符
                Bytes.toBytes("3000")); //值
        // 定义一个扫描器
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String name = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("ename")));
            String val = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("sal")));
            System.out.println(name+"------"+val);
        }
        table.close();
    }

    /**
     （*）列名前缀过滤器: ColumnPrefixFilter
                查询所有员工的姓名
                  select ename from emp;
     **/
    @Test
    public void testFilter2 () throws Exception{
        //客户端
        HTable table = new HTable(conf, "emp");

        // 列名前缀过滤器: ColumnPrefixFilter  只能指定一个列
        ColumnPrefixFilter singleColumnValueFilter = new ColumnPrefixFilter(
                Bytes.toBytes("ename")); //列名
        // 定义一个扫描器
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String name = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("ename")));
            // 这一列不进行查询
            String val = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("sal")));
            System.out.println(name+"------"+val);
        }
        table.close();
    }

    /**
     （*）多个列名前缀过滤器:MultipleColumnPrefixFilter
          查询所有员工的姓名和薪水
             select ename ,sal from emp;
     **/
    @Test
    public void testFilter3 () throws Exception{
        //客户端
        HTable table = new HTable(conf, "emp");

        // 创建多个列 一个二维数组
        byte[][] names = {Bytes.toBytes("ename"),Bytes.toBytes("sal")};

        // 多个列名前缀过滤器:MultipleColumnPrefixFilter
        MultipleColumnPrefixFilter singleColumnValueFilter = new MultipleColumnPrefixFilter(names); //列名
        // 定义一个扫描器
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String name = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("ename")));
            // 这一列不进行查询
            String val = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("sal")));
            System.out.println(name+"------"+val);
        }
        table.close();
    }

    /**
     （*）行键过滤器：根据rowkey进行查询
     **/
    @Test
    public void testFilter4 () throws Exception{
        //客户端
        HTable table = new HTable(conf, "emp");
        // rowKey过滤器:RowFilter
        RowFilter singleColumnValueFilter = new RowFilter(
                 CompareOp.EQUAL, // 比较运算符
                 new RegexStringComparator("7839")); // rowkey 可以使用正则表达式
        // 定义一个扫描器
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String name = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("ename")));
            // 这一列不进行查询
            String val = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("sal")));
            System.out.println(name+"------"+val);
        }
        table.close();
    }


    /**
     * @Author Jackson_J
     * @Description  组合使用过滤器
     *     （*）组合多个过滤器: 查询工资等于3000的员工姓名
     * 			1、第一个过滤器：列值过滤器
     * 			2、第二个过滤器：列名前缀过滤器
     * @Date 23:16 2019/2/23
     * @Param []
     * @return void
     **/
    @Test
    public void test5 () throws Exception{
        //客户端
        HTable table = new HTable(conf, "emp");
        // 第一个过滤器：列值过滤器 工资 等于 3000
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes("empinfo"),
                Bytes.toBytes("sal"),
                CompareOp.EQUAL, // 比较运算符
                Bytes.toBytes("3000"));

        // 第二个过滤器：列名前缀过滤器
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("ename"));
        // 定义一个扫描器
        Scan scan = new Scan();
        // 设置过滤器
        FilterList filterList = new FilterList(Operator.MUST_PASS_ALL); // 参数是过滤器间的关系 and or
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(columnPrefixFilter);
        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String name = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("ename")));
            // 这一列不进行查询
            String val = Bytes.toString(result.getValue(Bytes.toBytes("empinfo"),Bytes.toBytes("sal")));
            System.out.println(name+"------"+val);
        }
        table.close();
    }
}
