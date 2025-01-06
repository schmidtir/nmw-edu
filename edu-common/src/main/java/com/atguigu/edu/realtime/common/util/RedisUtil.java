package com.atguigu.edu.realtime.common.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    /**
     * 异步读
     */
    public static JSONObject readDimAsync( StatefulRedisConnection<String, String> connection , String dimTableName , String dimRowkey ){
        try {
            String key = getKey( dimTableName , dimRowkey );
            RedisAsyncCommands<String, String> asyncCommands = connection.async();
            String dimJsonStr  = asyncCommands.get(key).get();

            return JSON.parseObject( dimJsonStr );
        }catch ( Exception e) {
            e.printStackTrace();
            throw new RuntimeException(" 异步读取 Redis 失败") ;
        }

    }

    /**
     * 异步写
     */
    public static void writeDimAsync(StatefulRedisConnection<String, String> connection , String dimTableName , String dimRowkey , JSONObject dimJsonObj){
        String key = getKey( dimTableName , dimRowkey );
        RedisAsyncCommands<String, String> asyncCommands = connection.async();
        asyncCommands.setex( key , Constant.REDIS_DIM_EX_ONE_DAY, dimJsonObj.toJSONString()) ;
    }


    public static void closeConnection(StatefulRedisConnection<String, String> connection){
        if(connection != null && connection.isOpen()){
            connection.close();
        }
    }

    public static StatefulRedisConnection<String, String> getConnection(){
        RedisURI uri = RedisURI.Builder
                .redis(Constant.REDIS_HOST, Constant.REDIS_PORT)
                .build();
        RedisClient client = RedisClient.create(uri);
        StatefulRedisConnection<String, String> connection = client.connect();
        return connection ;
    }

    // 定义连接池
    private static JedisPool jedisPool ;

    static{
        JedisPoolConfig config = new JedisPoolConfig();

        config.setMaxTotal(100);
        config.setMaxIdle(10);
        config.setMinIdle(10);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setMaxWaitMillis(10 * 1000);

        jedisPool = new JedisPool( config , Constant.REDIS_HOST , Constant.REDIS_PORT);
    }

    /**
     * 使用连接池的方式
     * @return
     */
    public static Jedis getJedis(){
        return jedisPool.getResource() ;
    }

    public static void closeJedis(Jedis jedis ){
        jedis.close();
    }

    /**
     * 从redis中读取维度数据
     */
    public static JSONObject readDim( Jedis jedis , String dimTableName , String dimRowkey ){
        String key = getKey( dimTableName , dimRowkey );
        String dimJsonStr = jedis.get(key);

        return JSON.parseObject( dimJsonStr );
    }

    /**
     * 往Redis中写维度数据
     */
    public static void writeDim(Jedis jedis , String dimTableName , String dimRowkey , JSONObject dimJsonObj){
        String key = getKey( dimTableName , dimRowkey );

        jedis.setex( key , Constant.REDIS_DIM_EX_ONE_DAY, dimJsonObj.toJSONString()) ;
    }


    /**
     * 从Redis中删除维度数据
     */

    public static void deleteDim(Jedis jedis , String dimTableName , String dimRowkey ){
        String key = getKey( dimTableName , dimRowkey );
        jedis.del( key );
    }


    public static String getKey(String dimTableName , String dimRowkey ){
        return dimTableName + ":" + dimRowkey ;

    }
}
