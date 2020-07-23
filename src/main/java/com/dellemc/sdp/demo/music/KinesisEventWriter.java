package com.dellemc.sdp.demo.music;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class KinesisEventWriter implements SongEventGenerator.EventWriter, AutoCloseable {
    SongEventGenerator.Config config;
    AmazonKinesis kinesisClient;

    public KinesisEventWriter(SongEventGenerator.Config config) {
        this.config = config;
        kinesisClient = AmazonKinesisClientBuilder.standard().withCredentials(new ProfileCredentialsProvider(config.getAwsProfile())).build();
    }

    @Override
    public void writeEvent(String routingKey, String body) {
        kinesisClient.putRecord(config.getStream(), ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)), routingKey);
    }

    @Override
    public synchronized void close() {
        try {
            if (kinesisClient != null) kinesisClient.shutdown();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        kinesisClient = null;
    }
}
