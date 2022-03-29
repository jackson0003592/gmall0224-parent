package com.atguigu.gmall.realtime.beans;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Author: Felix
 * Date: 2021/8/11
 * Desc: 当处理向ClickHouse写入数据属性的时候，如果属性不需要保存到CK，那么可以通过该注解标记
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
