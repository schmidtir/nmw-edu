package com.atguigu.edu.realtime.common.bean;

/* *
 * Package Name: com.atguigu.edu.realtime.common.bean
 * Author : Kevin
 * Create Date ：2025/1/9
 * Create Time ：9:48
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.annotation.JSONField;
import lombok.*;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeOrderProvincesBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 省份 ID
    String provinceId;

    // 省份名称
    String provinceName;

    // 用户 ID
    @JSONField(serialize=false)
    String userId;

    // 订单总额
    BigDecimal orderTotalAmount;

    // 下单独立用户数
    Long orderUuCount;

    // 订单数
    Long orderCount;

    // 时间戳
    @JSONField(serialize=false)
    Long ts;

}
