package com.bigData.HDFS.RPCServer;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HDFS.RPCServer
 * @Author: 15568
 * @CreateTime: 2018-12-25 22:10
 * @Description:
 *
 */
public class MyHadoopRPCServerImpl implements MyHadoopRPCServer {
    public String sayHello(String name) {
        System.out.println("************************************************");
        return "Hello " +name;
    }

    public long getProtocolVersion(String s, long l) throws IOException {
        return versionID;
    }

    /**
     * 定义签名的方法
     * @param s
     * @param l
     * @param i
     * @return
     * @throws IOException
     */
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature(versionID,null);
    }
}
