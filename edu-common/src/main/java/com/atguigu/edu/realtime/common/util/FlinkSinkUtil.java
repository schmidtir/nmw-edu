package com.atguigu.edu.realtime.common.util;

/* *
 * Package Name: com.atguigu.edu.realtime.common.util
 * Author : Kevin
 * Create Date ：2025/1/6
 * Create Time ：17:00
 * TODO     Flink Sink 的工具类
 * <p>
 * version: 0.0.1.0
 */


import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcessDwd;
import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static com.atguigu.edu.realtime.common.constant.Constant.*;

public class FlinkSinkUtil {

    public static DorisSink<String> getDorisSink(String database , String table ){
        // 上游是 json 写入时，需要开启配置
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");

        DorisSink<String> dorisSink = DorisSink.<String>builder()
                .setDorisReadOptions(
                        DorisReadOptions.builder().build()
                )
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder()
                                .setLabelPrefix("label-prefix" + System.currentTimeMillis())
                                .setStreamLoadProp(props)
                                .setDeletable(true)
                                .enable2PC()
                                //.disable2PC()
                                .build()
                )
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes( Constant.DORIS_FENODES )
                                .setTableIdentifier(database + "." + table )
                                .setUsername(Constant.DORIS_USERNAME)
                                .setPassword(Constant.DORIS_PASSWORD)
                                .build()
                )
                .setSerializer(new SimpleStringSerializer())
                .build();

        return dorisSink ;
    }


    public static KafkaSink<String> getKafkaSink( String topic ){
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic( topic )
                                .setValueSerializationSchema(
                                        new SimpleStringSchema()
                                )
                                .build()
                )
                .setDeliveryGuarantee( DeliveryGuarantee.AT_LEAST_ONCE)
                //.setDeliveryGuarantee( DeliveryGuarantee.EXACTLY_ONCE)
                //.setTransactionalIdPrefix("gmall_realtime_" + System.currentTimeMillis())
                //  检查点超时时间 <=  生产者事务超时时间 <= broker事务超时时间
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG , "600000")
                .build();

        return kafkaSink ;
    }

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getDwdKafkaSink() {

        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> splitSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers( KAFKA_BROKERS )
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tup2
                                    , KafkaSinkContext kafkaSinkContext
                                    , Long aLong) {

                                // 获取写入的数据
                                JSONObject jsonObj = tup2.f0;

                                // 获取要写入的主题
                                TableProcessDwd tableProcessDwd = tup2.f1;
                                String topic = tableProcessDwd.getSinkTable();


                                ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic
                                        , jsonObj.toJSONString().getBytes(StandardCharsets.UTF_8));
                                /*
                                 * 生产者写入 kafka 的分区策略
                                 * 1. 数据写到指定的分区流
                                 * 2. 根据key取hash值，依据hash值决定写入哪个分区
                                 * 3. 粘性分区
                                 * */

                                return producerRecord;
                            }
                        }
                )
//                .setDeliveryGuarantee( DeliveryGuarantee.AT_LEAST_ONCE )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("gmall_realtime_" + System.currentTimeMillis())
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 10 + "")
                .build();
        return splitSink;
    }
}
