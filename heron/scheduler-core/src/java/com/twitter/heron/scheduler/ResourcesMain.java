// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.ClusterConfig;
import com.twitter.heron.spi.common.ClusterDefaults;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.packing.IPacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.spi.utils.ReflectionUtils;
import com.twitter.heron.spi.utils.TopologyUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

public class ResourcesMain {
  private static final Logger LOG = Logger.getLogger(ResourcesMain.class.getName());

  public static void main(String... args) throws ParseException {
    // construct the options and help options first.
    Options options = constructOptions();
    Options helpOptions = constructHelpOptions();

    // parse the options
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(helpOptions, args, true);

    // print help, if we receive wrong set of arguments
    if (cmd.hasOption("h")) {
      usage(options);
      return;
    }

    // Now parse the required options
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      usage(options);
      throw new RuntimeException("Error parsing command line options: ", e);
    }

    Boolean verbose = cmd.hasOption("v");
    String cluster = cmd.getOptionValue("cluster");
    String role = cmd.getOptionValue("role");
    String environment = cmd.getOptionValue("environment");
    String topologyDefnFile = cmd.getOptionValue("topology_defn");
    String heronHome = cmd.getOptionValue("heron_home");
    String configPath = cmd.getOptionValue("config_path");
    String overrideConfigFile = cmd.getOptionValue("override_config_file");
    String releaseFile = cmd.getOptionValue("release_file");
    TopologyAPI.Topology topology = TopologyUtils.getTopology(topologyDefnFile);

    Config config = Config.newBuilder()
            .putAll(defaultConfigs(heronHome, configPath, releaseFile))
            .putAll(overrideConfigs(overrideConfigFile))
            .putAll(commandLineConfigs(cluster, role, environment, verbose))
            .putAll(topologyConfigs(topologyDefnFile, topology))
            .build();

    String packingClass = Context.packingClass(config);
    IPacking packing;
    try {
      // create an instance of the packing class
      packing = ReflectionUtils.newInstance(packingClass);

      Config runtime = Config.newBuilder()
          .put(Keys.topologyId(), topology.getId())
          .put(Keys.topologyName(), topology.getName())
          .put(Keys.topologyDefinition(), topology)
          .put(Keys.numContainers(), 1 + TopologyUtils.getNumContainers(topology))
          .build();

      packing.initialize(config, runtime);
      PackingPlan plan = packing.pack();

      System.out.println(formatContainerResources(topology.getName() ,plan));
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to instantiate instances", e);
    }
  }

  static String formatContainerResources(String topologyName, PackingPlan packingPlan) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    ObjectNode rootNode = mapper.createObjectNode();
    ObjectNode containerNode = mapper.createObjectNode();
    ObjectNode totalsNode = mapper.createObjectNode();

    rootNode.put("topology_name", topologyName);

    totalsNode.put("cpu", packingPlan.resource.cpu);
    totalsNode.put("ram", packingPlan.resource.ram);
    totalsNode.put("disk", packingPlan.resource.disk);

    for (Map.Entry<String, PackingPlan.ContainerPlan> entry : packingPlan.containers.entrySet()) {
      String containerId = entry.getKey();
      PackingPlan.ContainerPlan containerPlan = entry.getValue();
      Resource resources = containerPlan.resource;

      ObjectNode containerResources = mapper.createObjectNode();
      containerResources.put("cpu", resources.cpu);
      containerResources.put("ram", resources.ram);
      containerResources.put("disk", resources.disk);

      containerNode.putPOJO(containerId, containerResources);
    }

    rootNode.putPOJO("containers", containerNode);
    rootNode.putPOJO("totals", totalsNode);

    return mapper.writeValueAsString(rootNode);
  }

  /**
   * Load the topology config
   *
   * @param topology, proto in memory version of topology definition
   * @return config, the topology config
   */
  protected static Config topologyConfigs(String topologyDefnFile,
      TopologyAPI.Topology topology) {

    return Config.newBuilder()
        .put(Keys.topologyId(), topology.getId())
        .put(Keys.topologyName(), topology.getName())
        .put(Keys.topologyDefinitionFile(), topologyDefnFile)
        .build();
  }

  /**
   * Load the defaults config
   *
   * @param heronHome, directory of heron home
   * @param configPath, directory containing the config
   * @param releaseFile, release file containing build information
   * <p>
   * return config, the defaults config
   */
  protected static Config defaultConfigs(String heronHome, String configPath, String releaseFile) {
    return Config.newBuilder()
        .putAll(ClusterDefaults.getDefaults())
        .putAll(ClusterDefaults.getSandboxDefaults())
        .putAll(ClusterConfig.loadConfig(heronHome, configPath, releaseFile))
        .build();
  }

  /**
   * Load the override config from cli
   *
   * @param overrideConfigPath, override config file path
   * <p>
   * @return config, the override config
   */
  protected static Config overrideConfigs(String overrideConfigPath) {
    return Config.newBuilder()
        .putAll(ClusterConfig.loadOverrideConfig(overrideConfigPath))
        .build();
  }

  /**
   * Load the config parameters from the command line
   *
   * @param cluster, name of the cluster
   * @param role, user role
   * @param environ, user provided environment/tag
   * @param verbose, enable verbose logging
   * @return config, the command line config
   */
  protected static Config commandLineConfigs(String cluster,
                                             String role,
                                             String environ,
                                             Boolean verbose) {

    return Config.newBuilder()
        .put(Keys.cluster(), cluster)
        .put(Keys.role(), role)
        .put(Keys.environ(), environ)
        .put(Keys.verbose(), verbose)
        .build();
  }


    // Construct all required command line options
  private static Options constructOptions() {
    Options options = new Options();

    Option topologyDefn = Option.builder("n")
        .desc("Name of the topology")
        .longOpt("topology_defn")
        .hasArgs()
        .argName("topology definition file")
        .required()
        .build();

    Option configFile = Option.builder("p")
        .desc("Path of the config files")
        .longOpt("config_path")
        .hasArgs()
        .argName("config path")
        .build();

    Option heronHome = Option.builder("h")
        .desc("Path to heron home")
        .longOpt("heron_home")
        .hasArgs()
        .argName("heron home")
        .build();

    Option overrideConfigFile = Option.builder("c")
        .desc("override config file")
        .longOpt("override_config_file")
        .hasArgs()
        .argName("override config file")
        .build();

    Option releaseFile = Option.builder("r")
        .desc("release_file")
        .longOpt("release_file")
        .hasArgs()
        .argName("release file")
        .build();

    Option verbose = Option.builder("v")
        .desc("Enable debug logs")
        .longOpt("verbose")
        .build();

    options.addOption(topologyDefn);
    options.addOption(configFile);
    options.addOption(heronHome);
    options.addOption(releaseFile);
    options.addOption(overrideConfigFile);
    options.addOption(verbose);

    return options;
  }

    // construct command line help options
  private static Options constructHelpOptions() {
    Options options = new Options();
    Option help = Option.builder("h")
        .desc("List all options and their description")
        .longOpt("help")
        .build();

    options.addOption(help);
    return options;
  }

  // Print usage options
  private static void usage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("ResourcesMain", options);
  }
}
