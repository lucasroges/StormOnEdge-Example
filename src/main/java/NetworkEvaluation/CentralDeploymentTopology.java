package NetworkEvaluation;

/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import StormOnEdge.grouping.stream.*;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import backtype.storm.utils.NimbusClient;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.ExecutorStats;

import java.util.HashMap;

@SuppressWarnings("Duplicates")
public class CentralDeploymentTopology {
  private static final Log LOG = LogFactory.getLog(Main.class);

  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--debug", aliases = { "-d" }, usage = "enable debug")
  private boolean _debug = false;

  @Option(name = "--messageSizeByte", aliases = {"--messageSize" }, metaVar = "SIZE", 
          usage = "size of the messages generated in bytes")
  private int _messageSize = 100;

  @Option(name = "--localTaskGroup", aliases = {"--localGroup" }, metaVar = "LOCALGROUP", 
          usage = "number of initial local TaskGroup")
  private int _localGroup = 9;

  @Option(name = "--spoutParallel", aliases = {"--spout" }, metaVar = "SPOUT", 
          usage = "number of spouts to run inside a single TaskGroup")
  private int _spoutParallel = 2;

  @Option(name = "--boltParallelLocal", aliases = {"--boltLocal" }, metaVar = "BOLTLOCAL",
          usage = "number of bolts to run inside a single TaskGroup")
  private int _boltLocalParallel = 2;

  @Option(name = "--boltParallelGlobal", aliases = {"--boltGlobal" }, metaVar = "BOLTGLOBAL", 
          usage = "number of global bolts to run inside a single TaskGroup")
  private int _boltGlobalParallel = 2;

  @Option(name = "--numWorkers", aliases = {"--workers" }, metaVar = "WORKERS", 
          usage = "number of workers to use per topology")
  private int _numWorkers = 50;

  @Option(name = "--ackers", metaVar = "ACKERS", 
          usage = "number of acker bolts to launch per topology")
  private int _ackers = 1;

  @Option(name = "--maxSpoutPending", aliases = {"--maxPending" }, metaVar = "PENDING", 
          usage = "maximum number of pending messages per spout (only valid if acking is enabled)")
  private int _maxSpoutPending = -1;

  @Option(name = "--name", aliases = {"--topologyName" }, metaVar = "NAME", 
          usage = "base name of the topology (numbers may be appended to the end)")
  private String _name = "centralDeployment";

  @Option(name = "--ackEnabled", aliases = { "--ack" }, usage = "enable acking")
  private boolean _ackEnabled = false;

  @Option(name = "--pollFreqSec", aliases = {"--pollFreq" }, metaVar = "POLL", 
          usage = "How often should metrics be collected")
  private int _pollFreqSec = 30;

  @Option(name = "--testTimeSec", aliases = {"--testTime" }, metaVar = "TIME", 
          usage = "How long should the benchmark run for.")
  private int _testRunTimeSec = 5 * 60;

  @Option(name = "--sampleRateSec", aliases = {"--sampleRate" }, metaVar = "SAMPLE", 
          usage = "Sample rate for metrics (0-1).")
  private double _sampleRate = 0.3;

  @Option(name = "--multiplier", aliases = { "--mult" }, metaVar = "MULTIPLIER", 
          usage = "TBD")
  private int _multiplier = 1;

  private static class MetricsState {
    long transferred = 0;
    int slotsUsed = 0;
    long lastTime = 0;
  }

  private boolean printOnce = true;

  public void metrics(Nimbus.Client client, int size, int poll, int total) throws Exception {
    System.out.println(
        "status\ttopologies\ttotalSlots\tslotsUsed\ttotalExecutors\texecutorsWithMetrics\ttime\ttime-diff ms\ttransferred\tthroughput (MB/s)");
    MetricsState state = new MetricsState();
    long pollMs = poll * 1000;
    long now = System.currentTimeMillis();
    state.lastTime = now;
    long startTime = now;
    long cycle = 0;
    long sleepTime;
    long wakeupTime;
    while (metrics(client, size, now, state, "WAITING")) {
      now = System.currentTimeMillis();
      cycle = (now - startTime) / pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    }

    now = System.currentTimeMillis();
    cycle = (now - startTime) / pollMs;
    wakeupTime = startTime + (pollMs * (cycle + 1));
    sleepTime = wakeupTime - now;
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
    now = System.currentTimeMillis();
    long end = now + (total * 1000);
    do {

      /// one time print addition
      if (printOnce) {
        printExecutorLocation(client);
      }
      printOnce = false;
      ///

      metrics(client, size, now, state, "RUNNING");

      now = System.currentTimeMillis();
      cycle = (now - startTime) / pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    } while (now < end);
  }

  private void printExecutorLocation(Client client) throws Exception {
    ClusterSummary summary = client.getClusterInfo();
    StringBuilder executorBuilder = new StringBuilder();

    for (TopologySummary ts : summary.get_topologies()) {
      String id = ts.get_id();
      TopologyInfo info = client.getTopologyInfo(id);

      executorBuilder.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
      for (ExecutorSummary es : info.get_executors()) {
        executorBuilder.append(es.get_executor_info().get_task_start() + "," + es.get_component_id() + "," + es.get_host() + "\n");
      }
      executorBuilder.append("~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
    }

    System.out.println(executorBuilder.toString());
  }

  public boolean metrics(Nimbus.Client client, int size, long now, MetricsState state, String message) throws Exception {
    ClusterSummary summary = client.getClusterInfo();
    long time = now - state.lastTime;
    state.lastTime = now;
    int totalSlots = 0;
    int totalUsedSlots = 0;

    //////////
    // String namaSupervisor = "";
    for (SupervisorSummary sup : summary.get_supervisors()) {
      totalSlots += sup.get_num_workers();
      totalUsedSlots += sup.get_num_used_workers();
      // namaSupervisor = namaSupervisor + sup.get_host() + ",";
    }
    // System.out.println(namaSupervisor);

    int slotsUsedDiff = totalUsedSlots - state.slotsUsed;
    state.slotsUsed = totalUsedSlots;

    long totalTransferred = 0;
    int totalExecutors = 0;
    int executorsWithMetrics = 0;
    for (TopologySummary ts : summary.get_topologies()) {
      String id = ts.get_id();
      TopologyInfo info = client.getTopologyInfo(id);

      //// SOE Addition
      PerftestWriter.print(summary, info, new HashMap<String, Long>());
      ////

      for (ExecutorSummary es : info.get_executors()) {
        ExecutorStats stats = es.get_stats();
        totalExecutors++;
        if (stats != null) {
          Map<String, Map<String, Long>> transferred = stats.get_emitted();/* .get_transferred(); */
          if (transferred != null) {
            Map<String, Long> e2 = transferred.get(":all-time");
            if (e2 != null) {
              executorsWithMetrics++;
              // The SOL messages are always on the default stream, so just count those
              Long dflt = e2.get("default");
              if (dflt != null) {
                totalTransferred += dflt;
              }
            }
          }
        }
      }
    }
    // long transferredDiff = totalTransferred - state.transferred;
    state.transferred = totalTransferred;
    // double throughput = (transferredDiff == 0 || time == 0) ? 0.0 : (transferredDiff * size)/(1024.0 * 1024.0)/(time/1000.0);
    // System.out.println(message+"\t"+numTopologies+"\t"+totalSlots+"\t"+totalUsedSlots+"\t"+totalExecutors+"\t"+executorsWithMetrics+"\t"+now+"\t"+time+"\t"+transferredDiff+"\t"+throughput);
    System.out.println(message + "," + totalSlots + "," + totalUsedSlots + "," + totalExecutors + "," + executorsWithMetrics + "," + time);
    if ("WAITING".equals(message)) {
      // System.err.println(" !("+totalUsedSlots+" > 0 && "+slotsUsedDiff+" == 0 && "+totalExecutors+" > 0 && "+executorsWithMetrics+" >= "+totalExecutors+")");
    }
    return !(totalUsedSlots > 0 && slotsUsedDiff == 0 && totalExecutors > 0 && executorsWithMetrics >= totalExecutors);
  }


  public void realMain(String[] args) throws Exception {
    Map clusterConf = Utils.readStormConfig();
    clusterConf.putAll(Utils.readCommandLineOpts());
    Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      // if there's a problem in the command line,
      // you'll get this exception. this will report
      // an error message.
      System.err.println(e.getMessage());
      _help = true;
    }
    if (_help) {
      parser.printUsage(System.err);
      System.err.println();
      return;
    }
    if (_numWorkers <= 0) {
      throw new IllegalArgumentException("Need at least one worker");
    }
    if (_name == null || _name.isEmpty()) {
      throw new IllegalArgumentException("name must be something");
    }
    if (!_ackEnabled) {
      _ackers = 0;
    }

    try {

      int totalSpout = _spoutParallel * _localGroup * _multiplier;
      int totalLocalBolt = _boltLocalParallel * _localGroup * _multiplier;
      int totalLocalResultBolt = _localGroup * _multiplier;
      int totalGlobalBolt = _boltGlobalParallel * _multiplier;
      int totalGlobalResultBolt = _boltGlobalParallel * _multiplier;

      TopologyBuilder builder = new TopologyBuilder();

      builder.setSpout("messageSpoutLocal1", new SOESpout(_messageSize, _ackEnabled), totalSpout)
          .addConfiguration("group-name", "Local1")
          .addConfiguration("TaskPerCloud", _spoutParallel);
      builder.setBolt("messageBoltLocal1_1", new SOEBolt(), totalLocalBolt)
          .customGrouping("messageSpoutLocal1", new ZoneShuffleGrouping())
          .addConfiguration("group-name", "Global1")
          .addConfiguration("TaskPerCloud", _boltLocalParallel);
      builder.setBolt("messageBoltLocal1_2", new SOEBolt(), totalLocalBolt)
          .customGrouping("messageBoltLocal1_1", new ZoneShuffleGrouping())
          .addConfiguration("group-name", "Global1")
          .addConfiguration("TaskPerCloud", _boltLocalParallel);
      builder.setBolt("messageBoltLocal1_LocalResult", new SOEFinalBolt(), totalLocalResultBolt)
          .customGrouping("messageBoltLocal1_2", new ZoneShuffleGrouping())
          .addConfiguration("group-name", "Global1");

      builder.setSpout("messageSpoutLocal2", new SOESpout(_messageSize, _ackEnabled), totalSpout)
          .addConfiguration("group-name", "Local2")
          .addConfiguration("TaskPerCloud", _spoutParallel);
      builder.setBolt("messageBoltLocal2_1", new SOEBolt(), totalLocalBolt)
          .customGrouping("messageSpoutLocal2", new ZoneShuffleGrouping())
          .addConfiguration("group-name", "Global1")
          .addConfiguration("TaskPerCloud", _boltLocalParallel);
      builder.setBolt("messageBoltLocal2_2", new SOEBolt(), totalLocalBolt)
          .customGrouping("messageBoltLocal2_1", new ZoneShuffleGrouping())
          .addConfiguration("group-name", "Global1")
          .addConfiguration("TaskPerCloud", _boltLocalParallel);
      builder.setBolt("messageBoltLocal2_LocalResult", new SOEFinalBolt(), totalLocalResultBolt)
          .customGrouping("messageBoltLocal2_2", new ZoneShuffleGrouping())
          .addConfiguration("group-name", "Global1");

      builder.setBolt("messageBoltGlobal1_1A", new SOEBolt(), totalGlobalBolt).shuffleGrouping("messageBoltLocal1_1")
          .addConfiguration("group-name", "Global1")
          .addConfiguration("TaskPerCloud", _boltGlobalParallel);
      builder.setBolt("messageBoltGlobal1_1B", new SOEBolt(), totalGlobalBolt).shuffleGrouping("messageBoltLocal2_1")
          .addConfiguration("group-name", "Global1")
          .addConfiguration("TaskPerCloud", _boltGlobalParallel);
      builder.setBolt("messageBoltGlobal1_FG", new SOEBolt(), totalGlobalBolt)
          .fieldsGrouping("messageBoltGlobal1_1A", new Fields("fieldValue"))
          .fieldsGrouping("messageBoltGlobal1_1B", new Fields("fieldValue"))
          .addConfiguration("group-name", "Global1")
          .addConfiguration("TaskPerCloud", _boltGlobalParallel);
      builder.setBolt("messageBoltGlobal1_GlobalResult", new SOEFinalBolt(), totalGlobalResultBolt)
          .shuffleGrouping("messageBoltGlobal1_FG")
          .addConfiguration("group-name", "Global1");

      Config conf = new Config();
      conf.setDebug(_debug);
      conf.setNumWorkers(_numWorkers);
      conf.setNumAckers(_ackers);
      conf.setStatsSampleRate(_sampleRate);
      if (_maxSpoutPending > 0) {
        conf.setMaxSpoutPending(_maxSpoutPending);
      }

      StormSubmitter.submitTopologyWithProgressBar(_name, conf, builder.createTopology());

      metrics(client, _messageSize, _pollFreqSec, _testRunTimeSec);
    } catch (Exception e) {
      System.out.println(e.getMessage());
    } finally {
      LOG.info("KILLING " + _name);
      KillOptions killOpts = new KillOptions();
      killOpts.set_wait_secs(60);

      try {
        client.killTopologyWithOpts(_name, killOpts);
      } catch (Exception e) {
        LOG.error("Error trying to kill " + _name, e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    new CentralDeploymentTopology().realMain(args);
  }
}