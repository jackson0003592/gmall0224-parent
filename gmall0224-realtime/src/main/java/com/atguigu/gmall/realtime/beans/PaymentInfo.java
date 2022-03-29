package com.atguigu.gmall.realtime.beans;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Author: Felix
 * Date: 2021/8/10
 * Desc: 支付信息实体类
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}
