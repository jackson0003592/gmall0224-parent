package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

//    singleton = new Singleton();
//    指令1：获取singleton对象的内存地址
//    指令2：初始化singleton对象
//    指令3：将这块内存地址，指向引用变量singleton。
//
//    那么，这样我们就比较好理解，为什么要加入Volatile变量了。由于Volatile禁止JVM对指令进行重排序。所以创建对象的过程仍然会按照指令1-2-3的有序执行。
//    反之，如果没有Volatile关键字，假设线程A正常创建一个实例，那么指定执行的顺序可能2-1-3，当执行到指令1的时候，线程B执行getInstance方法，获取到的，可能是对象的一部分，或者是不正确的对象，程序可能就会报异常信息。
    private static volatile ThreadPoolExecutor instance;

    public static ThreadPoolExecutor getInstance() {
        if (instance == null) {
            synchronized (ThreadPoolUtil.class) {
                if (instance == null) {
                    System.out.println("---开辟线程池---");
                    instance = new ThreadPoolExecutor(
                            4, 10, 300, TimeUnit.SECONDS
                            , new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));

                }
            }
        }
        return instance;
    }
}
