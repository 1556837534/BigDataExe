package com.bigData.HDFS.RPCServer;

import org.apache.hadoop.ipc.VersionedProtocol;


/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HDFS.RPCServer
 * @Author: 15568
 * @CreateTime: 2018-12-25 22:07
 * @Description:
 *    1.要在 Hadoop的 RPC server 端运行 接口需要继承一个 VersionedProtocol 接口  这个接口用来创建对应的版本协议
 */
public interface MyHadoopRPCServer extends VersionedProtocol {
     // 定义自己的方法
    String sayHello(String name);
    // 定义自己的签名  就是一个 ID 编号 就能区分在客户端 调用的时候 调用的是哪个具体的实现
    // 变量的名称 必须 叫 versionID
    public static long versionID = 1l;

}
