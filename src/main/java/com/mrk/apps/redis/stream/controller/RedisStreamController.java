package com.mrk.apps.redis.stream.controller;

import com.mrk.apps.redis.stream.listener.RedisStreamListener;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;

@RestController
public class RedisStreamController {

    Logger LOGGER = LoggerFactory.getLogger(RedisStreamController.class);

    @Value("${spring.redis.stream.key}")
    private String streamKey;

    @Autowired
    StatefulRedisConnection statefulRedisConnection;

    @GetMapping("/addStream")
    public void addValueStream() {


        RedisAsyncCommands<String, String> commands = statefulRedisConnection.async();

        Map<String, String> body = Collections.singletonMap("POC", "Redis Stream Example");
        LOGGER.info(String.format("Adding message with body: %s", body));

        //adding value into stream
        commands.xadd(streamKey, body);





    }

}
