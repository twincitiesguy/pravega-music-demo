package com.dellemc.sdp.demo.music;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.local.InProcPravegaCluster;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.local.SingleNodeConfig;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

final class TestUtils {
    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    static final String PRAVEGA_CONTROLLER_URI = "tcp://127.0.0.1:9090";

    // Caller must close
    static InProcPravegaCluster startStandalone() throws Exception {
        // start Pravega stand-alone
        Properties standaloneProperties = new Properties();
        standaloneProperties.load(TestUtils.class.getResourceAsStream("/standalone-config.properties"));
        ServiceBuilderConfig config = ServiceBuilderConfig
                .builder()
                .include(standaloneProperties)
                .include(System.getProperties())
                .build();
        SingleNodeConfig conf = config.getConfig(SingleNodeConfig::builder);

        InProcPravegaCluster localPravega = LocalPravegaEmulator.builder()
                .controllerPort(conf.getControllerPort())
                .segmentStorePort(conf.getSegmentStorePort())
                .zkPort(conf.getZkPort())
                .restServerPort(conf.getRestServerPort())
                .enableRestServer(conf.isEnableRestServer())
                .enableAuth(conf.isEnableAuth())
                .enableTls(conf.isEnableTls())
                .certFile(conf.getCertFile())
                .keyFile(conf.getKeyFile())
                .enableTlsReload(conf.isEnableSegmentStoreTlsReload())
                .jksKeyFile(conf.getKeyStoreJKS())
                .jksTrustFile(conf.getTrustStoreJKS())
                .keyPasswordFile(conf.getKeyStoreJKSPasswordFile())
                .passwdFile(conf.getPasswdFile())
                .userName(conf.getUserName())
                .passwd(conf.getPasswd())
                .build()
                .getInProcPravegaCluster();

        log.warn("Starting Pravega Emulator with ports: ZK port {}, controllerPort {}, SegmentStorePort {}",
                conf.getZkPort(), conf.getControllerPort(), conf.getSegmentStorePort());

        localPravega.start();

        log.warn("Pravega Sandbox is running locally now. You could access it at {}:{}",
                "127.0.0.1", conf.getControllerPort());

        return localPravega;
    }

    static void createStreams(ClientConfig clientConfig, String scope, String... streams) {
        // create stream manager
        try (StreamManager streamManager = StreamManager.create(clientConfig)) {

            // create scope
            streamManager.createScope(scope);

            // configure stream
            StreamConfiguration.StreamConfigurationBuilder streamBuilder = StreamConfiguration.builder();
            streamBuilder.scalingPolicy(ScalingPolicy.byEventRate(20, 2, 1));

            for (String stream : streams) {
                streamManager.createStream(scope, stream, streamBuilder.build());
            }
        }
    }

    private TestUtils() {
    }
}
