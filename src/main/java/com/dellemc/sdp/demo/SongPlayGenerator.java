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
package com.dellemc.sdp.demo;

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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A simple app that writes random music plays to a stream
 */
public class SongPlayGenerator implements Runnable {
    private static Logger log = LoggerFactory.getLogger(SongPlayGenerator.class);

    private static final String SONG_MAP_RESOURCE = "/songs.lst";
    public static final int DEFAULT_MIN_XPUT = 1; // per second
    public static final int DEFAULT_MAX_XPUT = 20; // per second
    public static final int DEFAULT_XPUT_INTERVAL = 20; // seconds

    private static List<String> _songList;
    private static Map<String, String> _artistMap;

    static Map<String, String> getArtistMap() {
        if (_artistMap == null) {
            synchronized (SongPlayGenerator.class) {
                if (_artistMap == null) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(SongPlayGenerator.class.getResourceAsStream(SONG_MAP_RESOURCE)));
                    _artistMap = reader.lines().collect(Collectors.toMap(s -> s.split("::")[0], s -> s.split("::")[1]));
                }
            }
        }
        return _artistMap;
    }

    static List<String> getSongList() {
        if (_songList == null) {
            synchronized (SongPlayGenerator.class) {
                if (_songList == null) {
                    _songList = new ArrayList<>(getArtistMap().keySet());
                }
            }
        }
        return _songList;
    }

    private Config config;
    private Random random;
    private int currentXput;
    private long lastIntervalChangeTime;
    private AtomicBoolean running = new AtomicBoolean();

    public SongPlayGenerator(Config config) {
        this.config = config;
        this.random = new Random();
        verifyXput(); // last change time will be 0 here, so this will set an initial xput
    }

    public void run() {
        running.set(true);

        // create stream
        ClientConfig clientConfig = createClientConfig();
        createStream(clientConfig);

        // create client factory and event writer
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(config.getScope(), clientConfig);
             EventStreamWriter<String> writer = clientFactory.createEventWriter(
                     config.getStream(), new UTF8StringSerializer(), EventWriterConfig.builder().build())) {

            // loop until stopped
            while (running.get()) {

                // use the player ID as the routing key (guarantees order for each player)
                String playerId = generatePlayerId();
                String message = generatePlayMessage(playerId);
                log.info("Writing message size: {} to stream {} / {}",
                        message.length(), config.getScope(), config.getStream());
                writer.writeEvent(playerId, message);
                verifyXput();
                throttle();
            }
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
                    .scalingPolicy(ScalingPolicy.byEventRate(3, 2, 1))
                    .build();
            streamManager.createStream(config.getScope(), config.getStream(), streamConfiguration);
        }
    }

    String generatePlayerId() {

        // just generate a random integer between 1 and 10,000
        return "" + (random.nextInt(10000) + 1);
    }

    String generatePlayMessage(String playerId) {

        // pull a random song from the song list
        String song = getSongList().get(random.nextInt(getSongList().size()));

        // get the artist
        String artist = getArtistMap().get(song);
        return "{" +
                "\"playerId\": \"" + playerId + "\"," +
                "\"song\": \"" + song + "\"," +
                "\"artist\": \"" + artist + "\"" +
                "}";
    }

    void verifyXput() {
        if (System.currentTimeMillis() - lastIntervalChangeTime > config.getXputInterval() * 1000) {

            // time to change up the xput
            currentXput = randomizeXput();
            lastIntervalChangeTime = System.currentTimeMillis();
        }
    }

    int randomizeXput() {
        if (config.getMaxXput() > config.getMinXput())
            return random.nextInt(config.getMaxXput() - config.getMinXput()) + config.getMinXput();
        else return config.getMinXput();
    }

    void throttle() {
        long sleepTime = 1000 / currentXput;
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            log.warn("interrupted while throttling", e);
        }
    }

    static class Config {
        String controllerEndpoint;
        String scope;
        String stream;
        boolean useKeycloak;
        int minXput = DEFAULT_MIN_XPUT;
        int maxXput = DEFAULT_MAX_XPUT;
        int xputInterval = DEFAULT_XPUT_INTERVAL;

        public Config() {
        }

        public Config(String controllerEndpoint, String scope, String stream, boolean useKeycloak, int minXput, int maxXput, int xputInterval) {
            if (controllerEndpoint == null || controllerEndpoint.trim().length() == 0)
                throw new IllegalArgumentException("controller endpoint is required");
            if (scope == null || scope.trim().length() == 0) throw new IllegalArgumentException("scope is required");
            if (stream == null || stream.trim().length() == 0) throw new IllegalArgumentException("stream is required");
            if (minXput <= 0) throw new IllegalArgumentException("min xput must be greater than 0");
            if (minXput > maxXput)
                throw new IllegalArgumentException("max xput must be greater than or equal to min xput");
            if (xputInterval <= 0) throw new IllegalArgumentException("xput interval must be greater than 0");
            this.controllerEndpoint = controllerEndpoint;
            this.scope = scope;
            this.stream = stream;
            this.useKeycloak = useKeycloak;
            this.minXput = minXput;
            this.maxXput = maxXput;
            this.xputInterval = xputInterval;
        }

        public String getControllerEndpoint() {
            return controllerEndpoint;
        }

        public void setControllerEndpoint(String controllerEndpoint) {
            this.controllerEndpoint = controllerEndpoint;
        }

        public String getScope() {
            return scope;
        }

        public void setScope(String scope) {
            this.scope = scope;
        }

        public String getStream() {
            return stream;
        }

        public void setStream(String stream) {
            this.stream = stream;
        }

        public boolean isUseKeycloak() {
            return useKeycloak;
        }

        public void setUseKeycloak(boolean useKeycloak) {
            this.useKeycloak = useKeycloak;
        }

        public int getMinXput() {
            return minXput;
        }

        public void setMinXput(int minXput) {
            this.minXput = minXput;
        }

        public int getMaxXput() {
            return maxXput;
        }

        public void setMaxXput(int maxXput) {
            this.maxXput = maxXput;
        }

        public int getXputInterval() {
            return xputInterval;
        }

        public void setXputInterval(int xputInterval) {
            this.xputInterval = xputInterval;
        }
    }
}
