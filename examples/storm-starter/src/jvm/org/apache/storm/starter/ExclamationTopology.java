/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExclamationTopology.class);

    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String id = Thread.currentThread().getName().substring(27);
            String value = tuple.getString(0) + "-" + id.substring(1, id.length() / 2);
            List<Integer> ids = _collector.emit(tuple, new Values(value));

            if (ids.isEmpty()) {
                LOGGER.info("{}", value);
            }
//            LOGGER.info("Value {} is sent to {}", value, StringUtils.join(ids, ":"));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("bword"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        SpoutDeclarer spoutDeclarer = builder.setSpout("words", new TestWordSpout(), 4);
        BoltDeclarer boltDeclarer1 = builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("words");
        BoltDeclarer boltDeclarer2 = builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
        BoltDeclarer boltDeclarer3 = builder.setBolt("exclaim3", new ExclamationBolt(), 5).shuffleGrouping("words");

        Config conf = new Config();
//        conf.setDebug(true);

        if (null != args && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            LOGGER.info("Submit topology...");
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            LOGGER.info("Running OK...");
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}