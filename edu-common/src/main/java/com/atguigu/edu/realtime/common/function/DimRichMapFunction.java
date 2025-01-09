package com.atguigu.edu.realtime.common.function;

/* *
 * Package Name: com.atguigu.edu.realtime.common.function
 * Author : Kevin
 * Create Date ：2025/1/8
 * Create Time ：18:57
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import com.atguigu.edu.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

import static com.atguigu.edu.realtime.common.constant.Constant.HBASE_NAMESPACE;
import static org.apache.hadoop.hbase.util.CommonFSUtils.getTableName;

public abstract class DimRichMapFunction<T> extends RichMapFunction< T, T> implements DimFuction<T> {

    Connection connection = null;
    Jedis jedis = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeConnection(connection);
        RedisUtil.closeJedis( jedis );
    }

    @Override
    public T map(T bean) throws Exception {
        String tableName = getTableName();
        String rowKey = getRowKey( bean );

        // 先从 redis 中读取维度
        JSONObject dimJsonObj = RedisUtil.readDim( jedis, tableName, rowKey );
        if( dimJsonObj == null ){
            // redis 中没读到数据，则去 redis 中读数据
            dimJsonObj = HBaseUtil.getRow( connection, HBASE_NAMESPACE, tableName, rowKey );

            // 缓存到 redis
            RedisUtil.writeDim( jedis, tableName, rowKey, dimJsonObj );
        }

        addDim( bean, dimJsonObj );
        return bean;
    }

}
