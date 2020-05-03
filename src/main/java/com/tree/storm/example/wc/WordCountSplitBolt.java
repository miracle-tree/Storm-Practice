package com.tree.storm.example.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * TODO
 *
 * @author tree
 * @date 2020/5/3 16:29
 */
public class WordCountSplitBolt implements IRichBolt {

    // 初始化变量
    private OutputCollector collector;
    private String key;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // 初始化收集器
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {

        String value = input.getStringByField("wc-log");
        if(value != null && value.length() > 0){
            String[] split = value.split(" ");
            for(String word : split){
                collector.emit(new Values(word,1));
            }
            // 响应接受信息成功
            collector.ack(input);

            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("first","second"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
