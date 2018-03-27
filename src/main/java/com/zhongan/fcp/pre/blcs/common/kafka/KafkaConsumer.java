package com.zhongan.fcp.pre.blcs.common.kafka;

import com.zhongan.kafka.api.Consumer;
import com.zhongan.kafka.api.KafkaFactory;
import com.zhongan.kafka.api.MessageListener;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import javax.annotation.Resource;
import java.util.Properties;

@Setter
@Getter
@Slf4j
public class KafkaConsumer implements InitializingBean, DisposableBean {

    private String kafkaServer;
    private String groupId;
    private String topic;

    @Resource(name = "ZAKafkaFactory")
    private KafkaFactory kafkaFactory;

    @Resource(name = "KafkaMessageListener")
    private MessageListener messageListener;

    private Consumer consumer;

    private void start(){
        log.info("kafka consumer start!");
        consumer = kafkaFactory.createConsumer(getProperties());
        consumer.subscribe(topic, messageListener);
        consumer.start();
    }

    private Properties getProperties(){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return properties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    @Override
    public void destroy() throws Exception {
        if(null != consumer){
            consumer.shutdown();
        }
    }
}
