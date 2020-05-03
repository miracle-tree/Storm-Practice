package com.tree.storm.example.wc;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * TODO
 *
 * @author tree
 * @date 2020/5/3 16:23
 */
public class WordCountSpout implements IRichSpout {

    // 初始化参数
    private SpoutOutputCollector collector;
    private BufferedReader br;
    private String value;

    /**
     * 开启水龙头后执行
     */
    @Override
    public void nextTuple() {

        try{
            while((value = br.readLine()) != null){
                // 将读取到的数据直接发送到下一层级的bolt组件中
                this.collector.emit(new Values(value));
                Thread.sleep(20);
            }
        }catch (Exception e){
            System.out.println("从文件读取数据失败！");
        }

    }

    /**
     * 声明返回的tuple的类型
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wc-log"));
    }

    /**
     * 初始化Spout
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // 从文件中读取内容
        try {
            this.br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("e://wc.txt"))));
            this.collector = spoutOutputCollector;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
