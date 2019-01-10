package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Object;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class EmployeeSortReducer extends Reducer<Employee, NullWritable, LongWritable, Employee> {

	@Override
	protected void reduce(Employee k3, Iterable<NullWritable> v3,Context context)
			throws IOException, InterruptedException {
		context.write(new LongWritable(k3.getEmpNo()), k3);
	}

}
