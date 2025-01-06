package com.atguigu.edu.realtime.common.util;

/* *
 * Package Name: com.atguigu.edu.realtime.common.util
 * Author : Kevin
 * Create Date ：2025/1/6
 * Create Time ：17:00
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


public class HBaseUtil {

    /***
     * 获取连接
     */
    public static Connection getConnection()  {
        // 1. 创建配置对象
        Configuration conf = HBaseConfiguration.create();

        // 2. 添加配置参数
        conf.set("hbase.zookeeper.quorum",HBASE_ZOOKEEPER_QUORUM );

        // 3. 创建hbase的连接
        // 默认使用同步连接
        Connection connection;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("获取 HBase Connection 失败");
        }
        return connection;
    };

}
