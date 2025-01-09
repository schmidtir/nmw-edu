package com.atguigu.edu.realtime.common.bean;

/* *
 * Package Name: com.atguigu.edu.realtime.common.bean
 * Author : Kevin
 * Create Date ：2025/1/9
 * Create Time ：16:30
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.annotation.JSONField;

public class TrafficPageViewBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 设备 ID
    @JSONField(serialize=false)
    String mid;

    // 页面 ID
    @JSONField(serialize=false)
    String pageId;

    // 首页独立访客数
    Long homeUvCount;

    // 课程列表页独立访客数
    Long listUvCount;

    // 课程详情页独立访客数
    Long detailUvCount;

    // 时间戳
    @JSONField(serialize=false)
    Long ts;

}
