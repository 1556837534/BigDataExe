package com.bigData.HDFS.JDKProxy;

import org.apache.commons.lang.StringUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HDFS.proxy
 * @Author: 15568
 * @CreateTime: 2018-12-25 21:07
 * @Description: ${Description}
 */
public class test {
    public static void main(String[] args) {
        final MyService service = new MyServiceImpl();

        // 创建一个代理对象
        MyService myServiceProxy = (MyService) Proxy.newProxyInstance(test.class.getClassLoader(), service.getClass().getInterfaces(), new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                // 处理客户端的调用  这里只重写 sayHello 方法
                if (StringUtils.equals(method.getName(),"sayHello")) {
                    System.out.println("来源代理方法 Hello World");
                    return null;
                } else {
                    // 其他方法 通过真正对象去执行
                    return method.invoke(service,args);
                }
            }
        });

        // 通过代理对象调用业务方法
        myServiceProxy.sayHello();
        myServiceProxy.sayBye();
    }
}
