/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package com.dellemc.sdp.demo.music;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.keycloak.client.PravegaKeycloakCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple app that writes random music plays to a stream
 */
public class SongEventGenerator implements Runnable {
    private static Logger log = LoggerFactory.getLogger(SongEventGenerator.class);

    public static int DEFAULT_PLAYER_COUNT = 100;

    private static ObjectMapper mapper = new ObjectMapper();

    private Config config;
    private AtomicBoolean running = new AtomicBoolean();
    private List<SongPlayer> players = new ArrayList<>();
    private int futureEventThresholdMS = 5000; // only manage events that will emit before this threshold (5 seconds in the future)
    private ExecutorService taskService = Executors.newSingleThreadExecutor();

    public SongEventGenerator(Config config) {
        this.config = config;
    }

    public void run() {
        running.set(true);

        // create players
        for (int i = 0; i < config.getPlayerCount(); i++) {
            players.add(new SongPlayer(i + 1));
        }

        // create stream
        ClientConfig clientConfig = createClientConfig();
        createStream(clientConfig);

        // create client factory and event writer
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(config.getScope(), clientConfig);
             EventStreamWriter<String> writer = clientFactory.createEventWriter(
                     config.getStream(), new UTF8StringSerializer(), EventWriterConfig.builder().build())) {

            // loop until stopped
            while (running.get()) {

                // loop through all players and build a queue of future events based on emission time
                long now = System.currentTimeMillis();
                List<SongEvent> events = new ArrayList<>();
                for (SongPlayer player : players) {

                    // for all events from this player that will emit within the threshold, add them to the list
                    while (player.peekEvent().timestamp - now < futureEventThresholdMS) {
                        events.add(player.nextEvent());
                    }
                }

                // sort the events based on future emission time
                events.sort((o1, o2) -> (int) (o1.timestamp - o2.timestamp));

                // submit events to be emitted on time
                taskService.submit(new TimedEventWriterTask(writer, events));

                // sleep for a while
                Thread.sleep(futureEventThresholdMS / 5);
            }
        } catch (InterruptedException e) {
            log.error("interrupted while sleeping", e);
        } finally {
            taskService.shutdown();
        }
    }

    public void stop() {
        running.set(false);
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

    static class Config {
        String controllerEndpoint;
        String scope;
        String stream;
        boolean useKeycloak;
        int playerCount = DEFAULT_PLAYER_COUNT;

        public Config() {
        }

        public Config(String controllerEndpoint, String scope, String stream, boolean useKeycloak, int playerCount) {
            setControllerEndpoint(controllerEndpoint);
            setScope(scope);
            setStream(stream);
            setUseKeycloak(useKeycloak);
            setPlayerCount(playerCount);
        }

        public String getControllerEndpoint() {
            return controllerEndpoint;
        }

        public void setControllerEndpoint(String controllerEndpoint) {
            if (controllerEndpoint == null || controllerEndpoint.trim().length() == 0)
                throw new IllegalArgumentException("controller endpoint is required");
            this.controllerEndpoint = controllerEndpoint;
        }

        public String getScope() {
            return scope;
        }

        public void setScope(String scope) {
            if (scope == null || scope.trim().length() == 0) throw new IllegalArgumentException("scope is required");
            this.scope = scope;
        }

        public String getStream() {
            return stream;
        }

        public void setStream(String stream) {
            if (stream == null || stream.trim().length() == 0) throw new IllegalArgumentException("stream is required");
            this.stream = stream;
        }

        public boolean isUseKeycloak() {
            return useKeycloak;
        }

        public void setUseKeycloak(boolean useKeycloak) {
            this.useKeycloak = useKeycloak;
        }

        public int getPlayerCount() {
            return playerCount;
        }

        public void setPlayerCount(int playerCount) {
            if (playerCount < 1) throw new IllegalArgumentException("player count must be positive");
            this.playerCount = playerCount;
        }

        @Override
        public String toString() {
            return "Config{" +
                    "controllerEndpoint='" + controllerEndpoint + '\'' +
                    ", scope='" + scope + '\'' +
                    ", stream='" + stream + '\'' +
                    ", useKeycloak=" + useKeycloak +
                    ", playerCount=" + playerCount +
                    '}';
        }
    }

    class TimedEventWriterTask implements Runnable {
        private EventStreamWriter<String> writer;
        private List<SongEvent> events;

        /**
         * @param events a *sorted* list of events to write (sorted by future emission time)
         */
        public TimedEventWriterTask(EventStreamWriter<String> writer, List<SongEvent> events) {
            this.writer = writer;
            this.events = events;
        }

        @Override
        public void run() {
            for (SongEvent event : events) {
                try {

                    // only sleep if it's worth it (more than 5ms in future)
                    long waitMS = event.timestamp - System.currentTimeMillis();
                    if (waitMS > 5) {
                        Thread.sleep(waitMS);
                    }

                    // marshall event to JSON
                    // TODO: maybe move this out of the write task for efficiency
                    String json = mapper.writeValueAsString(event);

                    // use the player ID as the routing key (guarantees order for each player)
                    log.info("Writing message (key: {}, size: {}, timestamp: {}) to stream {} / {}",
                            event.playerId, json.length(), event.timestamp, config.getScope(), config.getStream());
                    log.debug("raw event: {}", json);
                    writer.writeEvent("" + event.playerId, json);
                } catch (InterruptedException e) {
                    log.warn("interrupted while sleeping", e);
                } catch (JsonProcessingException e) {
                    log.warn("error marshalling JSON", e);
                }
            }
        }
    }
}
