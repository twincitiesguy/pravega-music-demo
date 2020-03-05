package com.dellemc.sdp.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.local.InProcPravegaCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.UUID;

public class SongPlayGeneratorTest {
    private static final Logger logger = LoggerFactory.getLogger(SongPlayGeneratorTest.class);

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
    public void testFixedRate() throws Exception {
        int totalEvents = 20, xput = 1;

        SongPlayGenerator.Config config = new SongPlayGenerator.Config();
        config.setControllerEndpoint(TestUtils.PRAVEGA_CONTROLLER_URI);
        config.setScope(TEST_SCOPE);
        config.setStream(TEST_STREAM);
        config.setMinXput(xput);
        config.setMaxXput(xput);

        SongPlayGenerator generator = new SongPlayGenerator(config);
        new Thread(generator).start();

        // wait 20 seconds
        Thread.sleep(totalEvents * 1000 / xput);

        generator.stop();

        // make sure 20 events were written
        EventRead eventRead;
        int count = 0;
        do {
            eventRead = pravegaReader.readNextEvent(READ_TIMEOUT);
            if (eventRead.getEvent() != null) count++;
        } while (eventRead.getEvent() != null || eventRead.isCheckpoint());

        Assertions.assertEquals(totalEvents, count);
    }

    @Test
    public void testVariableRate() throws Exception {
        int minXput = 2, maxXput = 20, xputInterval = 6, numIntervals = 5;

        SongPlayGenerator.Config config = new SongPlayGenerator.Config();
        config.setControllerEndpoint(TestUtils.PRAVEGA_CONTROLLER_URI);
        config.setScope(TEST_SCOPE);
        config.setStream(TEST_STREAM);
        config.setMinXput(minXput);
        config.setMaxXput(maxXput);
        config.setXputInterval(xputInterval);

        SongPlayGenerator generator = new SongPlayGenerator(config);
        new Thread(generator).start();
        long startTime = System.currentTimeMillis();

        int totalEvents = 0;

        // for each expected interval, wait until the middle of the interval, verify xput bounds, and accumulate total events
        for (int i = 0; i < numIntervals; i++) {
            long intervalStartTime = startTime + (i * xputInterval * 1000); // i is zero-based
            long midIntervalTime = intervalStartTime + (xputInterval * 1000 / 2);

            // wait until we are in the middle of this interval
            Thread.sleep(midIntervalTime - System.currentTimeMillis());

            int currentXput = generator.getCurrentXput();

            // check that current xput is within range
            Assertions.assertTrue(currentXput >= minXput && currentXput <= maxXput,
                    "current xput is out of range (" + currentXput + ")");

            // accumulate expected total events
            totalEvents += currentXput * xputInterval;

            logger.info("checked interval {} -- xput: {}, interval length: {}, expected events after interval: {}",
                    i + 1, currentXput, xputInterval, totalEvents);
        }

        // wait until last interval is over
        long endTime = startTime + (xputInterval * numIntervals * 1000);
        Thread.sleep(endTime - System.currentTimeMillis());

        // stop generating events
        generator.stop();

        // make sure expected events were written
        EventRead eventRead;
        int count = 0;
        do {
            eventRead = pravegaReader.readNextEvent(READ_TIMEOUT);
            if (eventRead.getEvent() != null) count++;
        } while (eventRead.getEvent() != null || eventRead.isCheckpoint());

        int diff = Math.abs(totalEvents - count), margin = totalEvents / 50;
        logger.info("expected events: {}, actual events: {}, difference: {}, margin for error: {}",
                totalEvents, count, diff, margin);
        Assertions.assertTrue(diff <= margin,
                "total events is more than 2% off (expected: " + totalEvents + ", actual: " + count + ")");
    }
}
