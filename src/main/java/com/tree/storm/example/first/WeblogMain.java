package com.tree.storm.example.first;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * TODO
 *
 * @author tree
 * @date 2020/5/2 22:02
 */
public class WeblogMain {

    public static void main(String[] args) {

        // 创建topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // 指定spout
        topologyBuilder.setSpout("spout",new WeblogSpout(),1);
        topologyBuilder.setBolt("bolt",new WeblogBlot(),1);

        // 指定配置文件
        Config config = new Config();
        config.setNumWorkers(2);

        // 提交拓扑
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("weblogtopology", config, topologyBuilder.createTopology());
    }

}
