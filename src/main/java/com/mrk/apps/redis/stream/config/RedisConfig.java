package com.mrk.apps.redis.stream.config;

import com.mrk.apps.redis.stream.listener.RedisStreamListener;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class RedisConfig {

    private Logger LOGGER = LoggerFactory.getLogger(RedisConfig.class);

    @Value("${spring.redis.host}")
    private String redisHost;

    @Value("${spring.redis.port}")
    private int redisPort;

    @Value("${spring.redis.stream.key}")
    private String streamKey;

    @Value("${spring.redis.stream.consumer-group}")
    private String consumerGroup;


    @Bean
    public RedisClient redisClient() {
        RedisURI redisURI = RedisURI.Builder.redis(redisHost, redisPort)
                .build();
        return RedisClient.create(redisURI);
    }

    @Bean
    public StatefulRedisConnection<String, String> redisConnection() {
        return redisClient().connect();
    }

    /**
     * Register StreamListener Container and add the listener to consume the stream
     * messages.
     *
     * @param redisStreamListener
     * @param redisConnectionFactory
     * @return
     */
    @Bean
    public Subscription listener(RedisStreamListener redisStreamListener, RedisConnectionFactory
            redisConnectionFactory) throws UnknownHostException {
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container =
                StreamMessageListenerContainer.create(redisConnectionFactory);
        try {

            RedisAsyncCommands<String, String> redisAsyncCommands = redisConnection().async();
            redisAsyncCommands.xgroupCreate(XReadArgs.StreamOffset.from(streamKey, "0-0"), consumerGroup, XGroupCreateArgs.Builder.mkstream());

            // redisTemplate().opsForStream().createGroup(streamName,ReadOffset.
            //       from("0-0"),consumerGroupName);
        } catch (Exception ex) {
            LOGGER.info("Already Created {}", ex.getMessage());
        }
        // Subscription subscription = container.receive(StreamOffset.fromStart(deleteStreamKey), streamListener);
        LOGGER.info("Host Name : {}", InetAddress.getLocalHost().getHostName());
        Subscription subscription = container.receive(Consumer.from(consumerGroup, InetAddress.getLocalHost().getHostName()),
                StreamOffset.create(streamKey, ReadOffset.from(">")), redisStreamListener);
        // Subscription subscription = container.receive(StreamOffset.fromStart(streamName), streamListener);
        container.start();
        //List<MapRecord<String, Object, Object>> records =  redisTemplate().opsForStream().read(StreamOffset.fromStart(deleteStreamKey));
        //System.out.println(records.stream().findAny().get());

        return subscription;

    }
}


