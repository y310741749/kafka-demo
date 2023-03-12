package com.provider.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        //
        props.put("bootstrap.servers", "192.168.146.128:9092");
        props.put("acks", "all");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulatot缓冲区带线啊哦
        props.put("buffer.memory", 33554432);
//        //key,value的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.provider.partitioner.MyPartitioner");
        List<String> strings = Collections.singletonList("com.provider.interceptor.TimeInterceotor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,strings);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        List<Integer> collect = Stream.of(1, 2, 3).collect(Collectors.toList());
//        List<Integer> collect = Stream.of(2 3, 4).collect(Collectors.toList());
        for (int i = 0; i < 10; i++) {
            // producer.send(new ProducerRecord<>("first","hahaha"+i));
            producer.send(new ProducerRecord<>("ccc", "hehehe---" + i), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println(metadata.partition() + "--" + metadata.offset());
                }
            });
        }
        producer.close();

    }
}
