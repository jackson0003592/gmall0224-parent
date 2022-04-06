package com.atguigu.gmall.realtime.common;

public class GmallConfig {

    public static final String HBASE_SCHEMA = "GMALL0224_REALTIME";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:node2,node3,node4:2181";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://node4:8123/default";
}
