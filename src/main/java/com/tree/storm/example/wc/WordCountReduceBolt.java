package com.tree.storm.example.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * TODO
 *
 * @author tree
 * @date 2020/5/3 16:34
 */
public class WordCountReduceBolt implements IRichBolt {

    // 定义初始变量
    private OutputCollector collector;
    // 定义一个map用于存储所有的数据
    private Map<String,Integer> resultMap = new HashMap<String, Integer>();


    // 初始化
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            String word = tuple.getString(0);
            int count = tuple.getInteger(1);
            add(resultMap,word,count);
            collector.ack(tuple);
            print(resultMap);
            Thread.sleep(10);
        }catch (Exception e){

        }
    }

    private void print(Map<String, Integer> resultMap) {
        StringBuilder sb = new StringBuilder();
        Set<String> keys = resultMap.keySet();
        for(String key : keys){
            sb.append(key).append("->").append(resultMap.get(key)).append(",");
        }
        System.out.println(sb.toString());
    }

    private void add(Map<String,Integer> map,String key,int count){
        map.put(key,map.getOrDefault(key,0)+count);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
