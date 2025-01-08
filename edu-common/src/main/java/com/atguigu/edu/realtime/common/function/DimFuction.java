package com.atguigu.edu.realtime.common.function;

/* *
 * Package Name: com.atguigu.edu.realtime.common.function
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：18:58
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;

public interface DimFuction<T> {
    String getTableName();
    String getRowKey( T bean );
    void addDim(T bean, JSONObject dimJsonObj);
}
