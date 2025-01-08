package com.atguigu.edu.realtime.common.bean;

/* *
 * Package Name: com.atguigu.edu.realtime.common.bean
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：17:09
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
public class TradeOrderCoursesWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 课程 ID
    String courseId;

    // 课程名称
    String courseName;

    // 科目 ID
    String subjectId;

    // 科目名称
    String subjectName;

    // 类别 ID
    String categoryId;

    // 类别名称
    String categoryName;

    // 下单总金额
    BigDecimal orderTotalAmount;

    // 时间戳
    @JSONField(serialize=false)
    Long ts;

}
