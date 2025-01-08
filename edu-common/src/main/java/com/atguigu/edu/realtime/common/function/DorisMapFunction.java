package com.atguigu.edu.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

public class DorisMapFunction<T> implements MapFunction< T , String > {
    @Override
    public String map(T value) throws Exception {

        SerializeConfig serializeConfig = new SerializeConfig();
        serializeConfig.setPropertyNamingStrategy( PropertyNamingStrategy.SnakeCase);
        String jsonString = JSON.toJSONString(value, serializeConfig);
        return jsonString;
    }
}