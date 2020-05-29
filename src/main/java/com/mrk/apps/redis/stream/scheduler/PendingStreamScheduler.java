package com.mrk.apps.redis.stream.scheduler;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;

@EnableScheduling
@Component
@Slf4j
public class PendingStreamScheduler {

    @Autowired
    StatefulRedisConnection statefulRedisConnection;

    @Autowired
    StringRedisTemplate template;

    @Value("${spring.redis.stream.key}")
    private String streamName;

    @Value("${spring.redis.stream.consumer-group}")
    private String consumerGroupName;

    @Value("${spring.redis.stream.consumer-name}")
    private String consumerName;


    @Scheduled(fixedRate = 5000)
    public void readPendingStreams() throws ExecutionException, InterruptedException {

        // creating asyc commands to claim messages from pending list
       RedisAsyncCommands<String, String> redisAsyncCommands = statefulRedisConnection.async();
        //RedisFuture f = redisAsyncCommands.xpending(streamKey, consumerGroup);

       // reading messages from pending list using string redis template
        PendingMessages messages = template.opsForStream().pending(streamName,consumerGroupName, Range.unbounded(), 5);
        for (PendingMessage message: messages) {
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .add(streamName)
                    .add(consumerGroupName)
                    .add(consumerName)
                    .add("10")
                    .add(message.getIdAsString());
            // using asyc coomands claiming pending message
            RedisFuture f = redisAsyncCommands.dispatch(CommandType.XCLAIM, new StatusOutput<>(StringCodec.UTF8), args);
            log.info("Message "+ message.getIdAsString() +" claimed by "+ consumerGroupName +":"+ consumerName);

            // using xrange method processing claimed message
            List<MapRecord<String, Object, Object>> messagesToProcess = template.opsForStream()
                    .range(streamName, Range.closed(message.getIdAsString(), message.getIdAsString()));

            MapRecord<String, Object, Object> msg = messagesToProcess.get(0);

            log.info("Message Successfully processed from Pending list: value :" + msg.getValue());

            //after successful processing, acknowledging the message inorder to delete it from pending list by redis.
            template.opsForStream().acknowledge(consumerGroupName, msg);
        }


    }
}
