package com.dellemc.sdp.demo.music;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.keycloak.client.PravegaKeycloakCredentials;

import java.net.URI;

public class PravegaEventWriter implements SongEventGenerator.EventWriter, AutoCloseable {
    SongEventGenerator.Config config;
    EventStreamClientFactory clientFactory;
    EventStreamWriter<String> writer;

    public PravegaEventWriter(SongEventGenerator.Config config) {
        this.config = config;

        // create stream
        ClientConfig clientConfig = createClientConfig();
        createStream(clientConfig);

        // create writer
        clientFactory = EventStreamClientFactory.withScope(config.getScope(), clientConfig);
        writer = clientFactory.createEventWriter(
                config.getStream(), new UTF8StringSerializer(), EventWriterConfig.builder().build());
    }

    @Override
    public void writeEvent(String routingKey, String body) {
        writer.writeEvent(routingKey, body);
    }

    @Override
    public synchronized void close() {
        try {
            if (writer != null) writer.close();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        writer = null;
        try {
            if (clientFactory != null) clientFactory.close();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        clientFactory = null;
    }

    ClientConfig createClientConfig() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder();
        builder.controllerURI(URI.create(config.getControllerEndpoint()));

        // Keycloak means we are using Streaming Data Platform
        if (config.isUseKeycloak()) {
            builder.credentials(new PravegaKeycloakCredentials());
        }

        return builder.build();
    }

    void createStream(ClientConfig clientConfig) {
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {

            // create the scope
            if (!config.isUseKeycloak()) // can't create a scope in SDP
                streamManager.createScope(config.getScope());

            // create the stream
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(5, 2, 2))
                    .build();
            streamManager.createStream(config.getScope(), config.getStream(), streamConfiguration);
        }
    }
}
