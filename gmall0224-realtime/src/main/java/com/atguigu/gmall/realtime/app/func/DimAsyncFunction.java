package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

/**
 *
 * @param <T>
 *     模板方法设计模式
 *  *      在父类中定义实现某一个功能的核心算法骨架，将具体的实现延迟的子类中去完成。
 *  *      子类在不改变父类核心算法骨架的前提下，每一个子类都可以有自己的实现。
 *
 *    模板方法设计模式：
 *      在父类中定义某一功能的核心算法骨架，将具体的实现延迟到子类去完成
 *      子类在不改变父类的核心算法骨架的前提下，每一个子类都可以有自己不同的实现
 *
 *
 *
 *  * -需要启动的进程
 *  *      zk、kafka、maxwell、hdfs、hbase、redis
 *  *      BaseDBApp、OrderWideApp
 *  * -执行流程
 *  *      >运行模拟生成业务数据的jar
 *  *      >会向业务数据库MySQL中插入生成业务数据
 *  *      >MySQL会将变化的数据放到Binlog中
 *  *      >Maxwell从Binlog中获取数据，将数据封装为json字符串发送到kafka的ods主题  ods_base_db_m
 *  *      >BaseDBApp从ods_base_db_m主题中读取数据，进行分流
 *  *          &事实----写回到kafka的dwd主题
 *  *          &维度----保存到phoenix的维度表中
 *  *      >OrderWideApp从dwd主题中获取订单和订单明细数据
 *  *      >使用intervalJoin对订单和订单明细进行双流join
 *  *      >将用户维度关联到订单宽表上
 *  *          *基本的维度关联
 *  *          *优化1：旁路缓存的优化
 *  *          *优化2：异步IO
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private ExecutorService executorService;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    String key = getKey(obj);
                    JSONObject jsonObject = DimUtil.getDimInfo(tableName, key);

                    if (jsonObject != null) {
                        join(obj, jsonObject);
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("维度异步查询耗时:" + (end - start) + "毫秒");
                    resultFuture.complete(Collections.singleton(obj));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("维度异步查询发生了异常");
                }
            }
        });
    }
}
