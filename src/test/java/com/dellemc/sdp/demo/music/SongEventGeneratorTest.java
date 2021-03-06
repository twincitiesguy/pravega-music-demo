package com.dellemc.sdp.demo.music;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.local.InProcPravegaCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SongEventGeneratorTest {
    private static final Logger log = LoggerFactory.getLogger(SongEventGeneratorTest.class);

    private static final String TEST_SCOPE = "pravega-demo";
    private static final String TEST_STREAM = "demo-test-stream";

    private static final long READ_TIMEOUT = 2000; // 2 seconds
    private static final String TEST_READER_GROUP = "song-play-test-readers";

    private static ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(TestUtils.PRAVEGA_CONTROLLER_URI)).build();
    private static InProcPravegaCluster localPravega;
    private static EventStreamClientFactory pravegaClientFactory;
    private static EventStreamWriter<String> pravegaWriter;
    private static EventStreamReader<String> pravegaReader;

    @BeforeAll
    public static void classSetup() throws Exception {
        localPravega = TestUtils.startStandalone();

        // initialize Pravega client
        pravegaClientFactory = initClient();

        // init test writer
        pravegaWriter = pravegaClientFactory.createEventWriter(TEST_STREAM,
                new UTF8StringSerializer(), EventWriterConfig.builder().build());

        // init test reader
        pravegaReader = pravegaClientFactory.createReader(UUID.randomUUID().toString(), TEST_READER_GROUP,
                new UTF8StringSerializer(), ReaderConfig.builder().build());
    }

    private static EventStreamClientFactory initClient() {
        // NOTE: in order to have consistent positions between readers and writers, we use separate streams for testing
        // Create and Query operations
        TestUtils.createStreams(clientConfig, TEST_SCOPE, TEST_STREAM);

        // create Create operation test reader group
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(TEST_SCOPE, TEST_STREAM)).build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(TEST_SCOPE, clientConfig)) {
            readerGroupManager.createReaderGroup(TEST_READER_GROUP, readerGroupConfig);
        }

        // create client factory
        return EventStreamClientFactory.withScope(TEST_SCOPE, clientConfig);
    }

    @AfterAll
    public static void classTearDown() throws Exception {
        // shut down client
        if (pravegaReader != null) pravegaReader.close();
        if (pravegaWriter != null) pravegaWriter.close();
        if (pravegaClientFactory != null) pravegaClientFactory.close();

        // shut down Pravega stand-alone
        if (localPravega != null) localPravega.close();
    }

    @Test
    public void testGenerator() throws Exception {
        SongEventGenerator.Config config = new SongEventGenerator.Config();
        config.setControllerEndpoint(TestUtils.PRAVEGA_CONTROLLER_URI);
        config.setScope(TEST_SCOPE);
        config.setStream(TEST_STREAM);

        SongEventGenerator generator = new SongEventGenerator(config);
        new Thread(generator).start();

        // wait 10 seconds
        Thread.sleep(10000);

        generator.stop();

        // make sure multiple events were written
        // we can't really predict the number
        EventRead eventRead;
        int count = 0;
        do {
            eventRead = pravegaReader.readNextEvent(READ_TIMEOUT);
            if (eventRead.getEvent() != null) count++;
        } while (eventRead.getEvent() != null || eventRead.isCheckpoint());

        log.info("emitted {} events in 10 seconds", count);
        assertTrue(count > 0);

    }
}
