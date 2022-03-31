package com.atguigu.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private static JedisPool jedisPool;
    public static Jedis getJedis(){
        if (jedisPool == null){
            initJedisPool();
        }

        System.out.println("----获取Redis链接-------");
        return jedisPool.getResource();
    }

    private static void initJedisPool(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(5);
        jedisPool = new JedisPool(jedisPoolConfig, "node4", 6379, 10000);
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        System.out.println(jedis.ping());
    }
}
