package com.tree.storm.example.first;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * TODO
 *
 * @author tree
 * @date 2020/5/2 21:47
 */
public class WeblogSpout implements IRichSpout {

    // 定义文件输入流
    private BufferedReader br;
    // 定义数据收集器
    private SpoutOutputCollector collector;
    // 定义单条数据
    private String value;

    // 发送单条数据？
    public void nextTuple() {
        try{
            // 如果当前读取数据不为空
            if((value = br.readLine()) != null){
                collector.emit(new Values(value));
            }
        }catch (Exception e){
            System.out.println("读取数据异常");
        }
    }

    // 初始化spout？
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {

        try {
            this.collector = collector;
            br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("e:/website.log"))));
        } catch (FileNotFoundException e) {
            System.out.println("获取数据失败");
        }

    }

    // 关闭spout？
    public void close() {

    }

    // 打开spout？
    public void activate() {

    }

    // 关闭spout？
    public void deactivate() {

    }

    // 信息发送成功？
    public void ack(Object o) {

    }

    // 信息发送失败？
    public void fail(Object o) {

    }

    // 声明输出属性？
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("web-log"));
    }

    // 获取组件信息？
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
