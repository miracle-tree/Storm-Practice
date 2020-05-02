package com.tree.storm.example.first;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * TODO
 *
 * @author tree
 * @date 2020/5/2 21:56
 */
public class WeblogBlot implements IRichBolt {

    // 定义收集器
    private OutputCollector collector;
    // 定义总的数量
    private int num;
    // 定义当前获取数据
    private String value;

    // 做准备工作
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    // 执行业务逻辑
    public void execute(Tuple tuple) {
        value = tuple.getStringByField("log");
        if(value != null){
            num++;
            System.err.println(Thread.currentThread().getName() + "lines  :" + num + "   session_id:" + value.split("\t")[1]);
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 关闭资源
    public void cleanup() {

    }

    // 声明
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    // 获取组件配置
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
