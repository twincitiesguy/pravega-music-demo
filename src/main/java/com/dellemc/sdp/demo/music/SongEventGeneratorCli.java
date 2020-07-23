package com.dellemc.sdp.demo.music;

import ch.qos.logback.classic.Level;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SongEventGeneratorCli {
    private static final Logger log = LoggerFactory.getLogger(SongEventGeneratorCli.class);

    static Options options() {
        Options options = new Options();

        options.addOption(Option.builder("c").longOpt("controller").desc("Service endpoint of the Pravega controller")
                .hasArg().argName("controller-uri").build());
        options.addOption(Option.builder("x").longOpt("scope").desc("The Pravega scope")
                .hasArg().argName("pravega-scope").build());
        options.addOption(Option.builder("s").longOpt("stream").desc("The Pravega stream name")
                .hasArg().argName("pravega-stream").build());

        options.addOption(Option.builder("k").longOpt("use-keycloak").desc("This enables Keycloak authentication for use with Streaming Data Platform. You must have a valid keycloak.json file in your home directory")
                .build());

        options.addOption(Option.builder("p").longOpt("players").desc("Number of players/users to simulate. Each player will simulate live user behavior. Default is " + SongEventGenerator.DEFAULT_PLAYER_COUNT + " (~8 events per second)")
                .hasArg().argName("num-players").build());

        options.addOption(Option.builder("v").longOpt("verbose").desc("Verbose logging").build());
        options.addOption(Option.builder("d").longOpt("debug").desc("Debug logging").build());

        options.addOption(Option.builder("z").longOpt("kinesis").desc("Write to Kinesis instead of Pravega (for testing)").build());
        options.addOption(Option.builder().longOpt("aws-profile").desc("When writing to Kinesis, the AWS CLI profile to use (configuration must be set for this profile)")
                .hasArg().argName("aws-profile").build());

        options.addOption(Option.builder("h").longOpt("help").desc("Print this help text").build());
        return options;
    }

    static SongEventGenerator.Config parseConfig(CommandLine commandLine) {
        SongEventGenerator.Config config = new SongEventGenerator.Config();

        if (commandLine.hasOption("players"))
            config.setPlayerCount(Integer.parseInt(commandLine.getOptionValue("players")));

        config.setControllerEndpoint(commandLine.getOptionValue('c'));
        config.setScope(commandLine.getOptionValue('x'));
        config.setStream(commandLine.getOptionValue('s'));
        config.setUseKeycloak(commandLine.hasOption('k'));
        config.setUseKinesis(commandLine.hasOption('z'));
        config.setAwsProfile(commandLine.getOptionValue("aws-profile"));

        return config;
    }

    public static void main(String[] args) throws Exception {
        CommandLine commandLine = new DefaultParser().parse(options(), args);

        // help text
        if (commandLine.hasOption('h')) {
            System.out.println("\n" + SongEventGenerator.class.getSimpleName() + " - generates random song plays and writes them to a stream\n");
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(SongEventGenerator.class.getSimpleName(), options(), true);
            System.out.println();
        } else {
            // set log level
            if (commandLine.hasOption('d')) {
                ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SongEventGeneratorCli.class.getPackage().getName())).setLevel(Level.DEBUG);
            } else if (commandLine.hasOption('v')) {
                ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SongEventGeneratorCli.class.getPackage().getName())).setLevel(Level.INFO);
            }

            SongEventGenerator.Config config = parseConfig(commandLine);
            log.info("parsed options:\n{}", config);
            SongEventGenerator writer = new SongEventGenerator(config);
            writer.run();
        }
    }
}
