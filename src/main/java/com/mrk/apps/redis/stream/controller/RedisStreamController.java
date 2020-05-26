package com.mrk.apps.redis.stream.controller;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;

@RestController
public class RedisStreamController {

    @Autowired
    RedisTemplate redisTemplate;

    @GetMapping("/addStream")
    public void addValueStream() {
        System.out.println("Inside controler");
//        Map<String, String> body = Collections.singletonMap("timeNew", LocalDateTime.now().toString());
//        StringRecord record = StreamRecords.string(body).withStreamKey("my-streamNEW");
//        redisTemplate.opsForStream().add(record);
        //return redisTemplate.opsForStream().add(StreamRecords.newRecord().in("streamx").ofObject(name)).getValue();

        RedisClient client = RedisClient.create("redis://localhost");
        StatefulRedisConnection<String, String> connection = client.connect();
        RedisAsyncCommands<String, String> commands = connection.async();

        Map<String, String> body = Collections.singletonMap("BENE_ID", "1234666");
        //LOGGER.info(String.format("Adding message with body: %s", body));

        commands.xadd("beneficiary_stream_new", body);
      // commands.xgroupCreate(StreamOffset.from("my-strea"), "bms", "$");




/*
        ObjectRecord<String, String> record = StreamRecords.newRecord()
                .in("beneficiary_stream_new")
                .ofObject("123456");

        System.out.println("After Stream");
        redisTemplate
                .opsForStream(new Jackson2HashMapper(true)).add(record);*/

    }

}
