package com.mrk.apps.redis.stream.listener;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RedisStreamListener implements StreamListener<String, MapRecord<String, String, String>> {


    @Autowired
    StringRedisTemplate template;

    @Value("${spring.redis.stream.consumer-group}")
    private String consumerGroupName;

    /**
     * Callback invoked on receiving a {@link }.
     *
     * @param message never {@literal null}.
     */
    @Override
    public void onMessage(MapRecord<String, String, String> message) {

        //first call reach here, processing message from redis stream based the configuration in config class. using xreadgroup
      log.info("Message Stream {} Id {} Value {}", message.getStream(), message.getId(), message.getValue());

        //after successful processing, we need to ack the message to remove from pending list.
        //commenting out here, as we are testing how to read the message from  pending list using scheduled task in PendingStreamScheduler.java
      //template.opsForStream().acknowledge(consumerGroupName, message);

    }
}
