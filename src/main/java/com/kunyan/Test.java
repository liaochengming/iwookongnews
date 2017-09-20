package com.kunyan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kunyan.conf.Platform;
import com.kunyan.util.ElasticUtil;
import com.nlp.util.EasyParser;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Administrator on 2017/9/7.
 */
public class Test {

    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

//        String[] a = new String[2];
//        a[0] = "1";
//        a[1] = "2";
//        System.out.println(a);
//        System.out.println(Arrays.toString(a));
//        String[] c = Arrays.toString(a).replace("[", "").replace("]", "").split(",");
//        System.out.println(c);
//        String s;

//        for(Platform p : Platform.values()){
//            System.out.println(p.toString());
//            //System.out.println(new String(p.getName().getBytes(), Charset.forName("UTF-8")));
//        }
//        System.out.println("中国");
//        System.out.println(Scheduler.getPlatformName(2));
//
//        new Thread(new Runnable() {
//            public void run() {
//                while (true){
//
//                    try {
//                        Thread.sleep(2000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }).start();
//
//        while (true){
//
//            System.out.println(Thread.currentThread().getName() + " => 1");
//            Thread.sleep(50);
//        }
        ElasticUtil elasticUtil = new ElasticUtil();
        String id = elasticUtil.selectId("http://stock.10jqka.com.cn/20170829/c600108082.shtml");
        System.out.println(id);
        elasticUtil.updateData(id,"{\"sit\":\"546\"}");
//        elasticUtil.client.prepareDelete("news","real_news","AV51zSr-m_GuypVVp6uP");
//

//        EasyParser easyParser =EasyParser.apply();
//        byte[] b = new byte[0];
//        System.out.println(new String(b, "UTF-8").equals(""));
//        JSONObject job = JSON.parseObject("{\"platform\": \"3\", \"hbase_table_name\": \"new_news\", \"task_id\": \"\", \"hbase_rowkey\": \"http://www.yicai.com/news/5345009.html\"}");
//        String platform = job.getString("update");
//        System.out.println(platform.endsWith("true"));
    }
}
