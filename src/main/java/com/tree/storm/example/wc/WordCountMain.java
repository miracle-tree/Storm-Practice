package com.tree.storm.example.wc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * TODO
 *
 * @author tree
 * @date 2020/5/3 16:43
 */
public class WordCountMain {

    public static void main(String[] args) {

        // 创建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 指定拓扑对应结构
        builder.setSpout("wc-spout",new WordCountSpout(),1);
        builder.setBolt("wc-split-bolt",new WordCountSplitBolt(),1).shuffleGrouping("wc-spout");
        builder.setBolt("wc-reduce-bolt",new WordCountReduceBolt(),1).shuffleGrouping("wc-split-bolt");

        Config config = new Config();
        config.setNumWorkers(4);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wc-topology", config, builder.createTopology());
    }

}
