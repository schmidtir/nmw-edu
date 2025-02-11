package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsInteractionCourseReviewWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 课程 ID
    String courseId;

    // 课程名称
    String courseName;

    // 用户总评分
    Long reviewTotalStars;

    // 评价用户数
    Long reviewUserCount;

    // 好评用户数
    Long goodReviewUserCount;

    // 时间戳
    @JSONField(serialize=false)
    Long ts;
}
