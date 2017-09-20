package com.kunyan.util;

import com.ibm.icu.text.CharsetDetector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/9/6.
 * 连接habse的工具类
 */
public class MyHbaseUtil {


    private Connection conn;


    public MyHbaseUtil(String root,String ip){
        this.conn = initConnect(root,ip);
    }


    public Connection initConnect(String root,String ip){

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.rootdir", root);
        hbaseConf.set("hbase.zookeeper.quorum", ip);

        Connection connection = null;

        try {
            connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("hbase create connect...");
        return connection;
    }


    public Table getTable(String tableName){
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public Map<String,String> query(Table table,String rowKey){

        Map<String,String> map = new HashMap<String,String>();
        Get get = new Get(rowKey.getBytes());

        try{

            byte[] time = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("time"));
            byte[] content = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"));
            byte[] title = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"));
            byte[] timeSpider = table.get(get).getValue(Bytes.toBytes("basic"),Bytes.toBytes("time_spider"));

            if (time == null && content == null) {
                return null;
            }

            if (content == null) content = new byte[0];
            if(time == null) time = new byte[0];
            if(title == null) title = new byte[0];

            if(new String(time, "UTF-8").equals("")){
                map.put("time",new String(timeSpider, "UTF-8"));
                map.put("content",new String(content, "UTF-8"));
                map.put("title",new String(title, "UTF-8"));
            }else{
                map.put("time",new String(time, "UTF-8"));
                map.put("content",new String(content, "UTF-8"));
                map.put("title",new String(title, "UTF-8"));
            }

            return map;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }

}
