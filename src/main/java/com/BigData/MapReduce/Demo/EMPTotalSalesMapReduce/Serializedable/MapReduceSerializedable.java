package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.Serializedable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.Serializedable
 * @Author: 15568
 * @CreateTime: 2018-12-31 17:22
 * @Description: mapReduce 自定义数据类型 需要实现 MapReduce 中的序列化接口 Writeable
 */
public class MapReduceSerializedable implements Writable {
    /**
     * 员工号
     */
    private int empNo;
    /**
     * 姓名
     */
    private String enName;
    /**
     * 职位
     */
    private String job;
    /**
     * 老板号
     */
    private int mgr;
    /**
     * 入职日期
     */
    private String hireDate;
    /**
     * 月薪
     */
    private int sal;
    /**
     * 奖金
     */
    private int comm;

    /**
     * 部门号
     */
    private int deptNo;

    public int getEmpNo() {
        return empNo;
    }

    public void setEmpNo(int empNo) {
        this.empNo = empNo;
    }

    public String getEnName() {
        return enName;
    }

    public void setEnName(String enName) {
        this.enName = enName;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public int getMgr() {
        return mgr;
    }

    public void setMgr(int mgr) {
        this.mgr = mgr;
    }

    public String getHireDate() {
        return hireDate;
    }

    public void setHireDate(String hireDate) {
        this.hireDate = hireDate;
    }

    public int getSal() {
        return sal;
    }

    public void setSal(int sal) {
        this.sal = sal;
    }

    public int getComm() {
        return comm;
    }

    public void setComm(int comm) {
        this.comm = comm;
    }

    public int getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(int deptNo) {
        this.deptNo = deptNo;
    }

    /**序列化的顺序一定要跟反序列化顺序一样 （序列化顺序）
     * 序列化  对象输出
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(this.empNo);
        dataOutput.writeUTF(this.enName);
        dataOutput.writeUTF(this.job);
        dataOutput.write(this.mgr);
        dataOutput.writeUTF(this.hireDate);
        dataOutput.write(this.sal);
        dataOutput.write(this.comm);
        dataOutput.write(this.deptNo);
    }

    /**序列化的顺序一定要跟反序列化顺序一样
     * 反序列化 把对象读入进来
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.empNo = dataInput.readInt();
        this.enName = dataInput.readUTF();
        this.job = dataInput.readUTF();
        this.mgr = dataInput.readInt();
        this.hireDate = dataInput.readUTF();
        this.sal = dataInput.readInt();
        this.comm = dataInput.readInt();
        this.deptNo = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "MapReduceSerializedable{" +
                "empNo=" + empNo +
                ", enName='" + enName + '\'' +
                ", job='" + job + '\'' +
                ", mgr=" + mgr +
                ", hireDate='" + hireDate + '\'' +
                ", sal=" + sal +
                ", comm=" + comm +
                ", deptNo=" + deptNo +
                '}';
    }
}
