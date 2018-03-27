package com.zhongan.fcp.pre.blcs.common.kafka;

import com.alibaba.fastjson.JSONObject;
import com.zhongan.blcs.client.BlcsAssit;
import com.zhongan.kafka.api.Action;
import com.zhongan.kafka.api.ConsumeContext;
import com.zhongan.kafka.api.MessageListener;
import com.zhongan.share.event.db.DBEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by huwenxuan on 2018/3/27.
 */
@Slf4j
public class KafkaMessageListener implements MessageListener<String, byte[]> {
    @Override
    public Action consume(ConsumerRecord<String, byte[]> consumerRecord, ConsumeContext consumeContext) {
        log.info("consume message!consumerRecord:{}, consumeContext:{}", consumerRecord, consumeContext);
        DBEvent dbEvent = BlcsAssit.parse(consumerRecord.value());
        Map<String, Object> fieldsMap = null;
        try {
            fieldsMap = BlcsAssit.getAfterMap(dbEvent);
        } catch (UnsupportedEncodingException e) {
            log.error("getAfterMap exception", e);
        }

        JSONObject jsonObject = (JSONObject) JSONObject.toJSON(fieldsMap);
        log.info("fieldsMap:{}==>jsonObject:{}", fieldsMap, jsonObject);
        /*TODO*/
        return Action.CommitMessage;
    }
}
