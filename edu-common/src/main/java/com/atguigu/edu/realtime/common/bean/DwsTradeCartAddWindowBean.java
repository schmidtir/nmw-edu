package com.atguigu.edu.realtime.common.bean;

/* *
 * Package Name: com.atguigu.edu.realtime.common.bean
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：11:07
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwsTradeCartAddWindowBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUvCount;

    @JSONField(serialize=false)
    Long ts;
}
