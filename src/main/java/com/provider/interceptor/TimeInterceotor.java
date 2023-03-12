package com.provider.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceotor implements ProducerInterceptor<String,String> {
    volatile int success = 0;
    volatile int error = 0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<>(record.topic(),record.partition(),record.key(),System.currentTimeMillis()+","+record.value());

    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(metadata!=null){
            success++;
        }else{
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("成功:"+success);
        System.out.println("失败:"+error);
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
