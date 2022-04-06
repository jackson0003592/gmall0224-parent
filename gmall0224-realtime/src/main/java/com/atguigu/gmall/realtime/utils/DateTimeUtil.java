package com.atguigu.gmall.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Author: Felix
 * Date: 2021/8/10
 * Desc:  日期转换的工具类
 *  SimpleDateFormat是线程不安全的
 *  在JDK1.8之后，使用DateTimeFormatter类替换
 */
public class DateTimeUtil {

    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    //将日期对象转换为字符串
    public static String toYMDHMS(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    //将字符串日期转换为时间毫秒数
    public static Long toTs(String dateStr){
        //Date====LocalDateTime  Calendar====Instant
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, dtf);
        long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }

    public static void main(String[] args) {
        System.out.println(ZoneId.systemDefault());
    }
}
