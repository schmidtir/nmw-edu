package com.atguigu.edu.realtime.common.util;

/* *
 * Package Name: com.atguigu.edu.realtime.common.util
 * Author : Kevin
 * Create Date ：2025/1/6
 * Create Time ：19:08
 * TODO
 * <p>
 * version: 0.0.1.0
 */


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class FlinkSourceUtil {

    public static void main(String[] args) {
        //
//        System.setProperty( HDFS_USER_NAME_CONFIG, HDFS_USER_NAME_VALUE );

        //
        Configuration conf = new Configuration();
        conf.setInteger( RestOptions.PORT, 10001 );

        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment( conf );
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = getKafkaSource("test1", "topic_db");

        DataStreamSource<String> kfkDs
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        kfkDs.print();
    }
    public static MySqlSource<String> getMySqlSource(String database, String table) {
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname( "hadoop102" )
                .port( 3306 )
                .databaseList( database )  // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 “.*” .
                .tableList( database+ "." + table ) // 设置捕获的表
                .username( "root" )
                .password( "000000" )
                .deserializer( new JsonDebeziumDeserializationSchema() )
                .startupOptions( StartupOptions.initial() )
                .build();

        return mysqlSource;
    }

    public static KafkaSource<String> getKafkaSource(String ckAndgroupId, String topic) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop105:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId( ckAndgroupId )
                .setTopics( topic )
                .setValueOnlyDeserializer(
                        /***
                         * 外连接-D会出现空值，这里的反序列化会报空指针异常
                         */
                        // new SimpleStringSchema()

                        // 自定义反序列化，支持 null 值
                        new DeserializationSchema<String>(){

                            @Override
                            public String deserialize(byte[] message) throws IOException {

                                // 判断是否为空
                                // 如果不为空，不进行反序列化
                                if( message != null ) {

                                    return new String(message, StandardCharsets.UTF_8);
                                };

                                // 如果为空，直接返回 null
                                return null;
                            }
                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false; // 无界流没有流的结束
                            }
                            @Override
                            public TypeInformation<String> getProducedType() {
                                return Types.STRING;
                            }
                        }

                )
                .setStartingOffsets(OffsetsInitializer.earliest())
//              .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        return kafkaSource;
    }
}
