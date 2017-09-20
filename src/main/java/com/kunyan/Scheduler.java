package com.kunyan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kunyan.entity.News;
import com.kunyan.util.ElasticUtil;
import com.kunyan.util.MyHbaseUtil;
import com.nlp.util.EasyParser;
import com.nlp.util.SegmentHan;
import de.mwvb.base.xml.XMLDocument;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 2017/9/6.
 *
 * 运行主类
 */
public class Scheduler {

    private static Table table = null;


    public static void main(String[] args) {


        if(args.length == 0){
            System.out.println("缺少配置文件");
            System.exit(0);
        }

        try{

            EasyParser easyParser =EasyParser.apply();


            XMLDocument doc = new XMLDocument();
            doc.loadFile(args[0]);

            ElasticUtil elasticUtil = new ElasticUtil(doc);

            String groupId = doc.selectSingleNode("xml/kafka/groupId").getText();
            String newsReceive = doc.selectSingleNode("xml/kafka/newsreceive").getText();
            String sentimentBack = doc.selectSingleNode("xml/kafka/sentiment_back").getText();
            String sentimentSend = doc.selectSingleNode("xml/kafka/sentiment_send").getText();
            String brokerList = doc.selectSingleNode("xml/kafka/brokerList").getText();

            String rootDir = doc.selectSingleNode("xml/hbase/rootDir").getText();
            String ip = doc.selectSingleNode("xml/hbase/ip").getText();
            MyHbaseUtil hbaseUtil = new MyHbaseUtil(rootDir,ip);

            Properties kafkaConsumerProps = new Properties();
            kafkaConsumerProps.put("bootstrap.servers", brokerList);
            kafkaConsumerProps.put("group.id", groupId);
            kafkaConsumerProps.put("enable.auto.commit", "true");
            kafkaConsumerProps.put("auto.commit.interval.ms", "1000");
            kafkaConsumerProps.put("session.timeout.ms", "30000");
            kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            Properties kafkaProducerProps  = new Properties();
            kafkaProducerProps.put("bootstrap.servers", brokerList);
            kafkaProducerProps.put("acks", "all");
            kafkaProducerProps.put("retries", 0);
            kafkaProducerProps.put("batch.size", 16384);
            kafkaProducerProps.put("linger.ms", 1);
            kafkaProducerProps.put("buffer.memory", 33554432);
            kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(kafkaConsumerProps);
            Producer<String, String> producer = new KafkaProducer<String,String>(kafkaProducerProps);

            consumer.subscribe(Arrays.asList(newsReceive, sentimentBack));

            String value;
            String newsType;
            String newsTitle;
            String newsSummary;
            String site;
            String newsUrl;
            String newsDate;
            String newsTime;
            List<String> industries;
            List<String> sections;
            List<String> stocks;
            Float positiveRate;
            Float neutralRate;
            Float passiveRate;
            String newsBody;
            String[] arr;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){

                    //System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                    value = record.value();
                    if(value.contains("hbase_table_name")){
                        //System.out.println(value);
                        analyzer(value,hbaseUtil,producer,sentimentSend,easyParser);
                    }else{

                        arr= value.split("<=");
                        if(arr.length == 14){

                            newsType = arr[4];
                            newsTitle = arr[5];
                            newsSummary = arr[6];
                            site = arr[7];
                            newsUrl = arr[0];
                            newsDate = arr[8];
                            newsTime = arr[9];
                            industries =  getList(arr[10]);
                            sections = getList(arr[11]);
                            stocks = getList(arr[12]);
                            positiveRate = Float.valueOf(arr[1]);
                            neutralRate = Float.valueOf(arr[2]);
                            passiveRate = Float.valueOf(arr[3]);
                            newsBody = arr[13];

                            News news =  new News(newsType,newsTitle,newsSummary,
                                    site,newsUrl,newsDate,
                                    newsTime,industries,sections,
                                    stocks,positiveRate,neutralRate,
                                    passiveRate,newsBody,true,new ArrayList<String>(),"",newsUrl);
                            insertES(news,elasticUtil);
                        }
                    }

                }

            }

        }catch (Exception e){
            e.printStackTrace();
        }


    }


    public static List<String> getList(String str){

        List<String> list = new ArrayList<String>();
        String[] arr = str.replace("[","").replace("]","").split(",");
        Collections.addAll(list, arr);
        return list;
    }


    public static void insertES(News news,ElasticUtil elasticUtil){

        String json = elasticUtil.CreateNews(news);
        elasticUtil.createIndex(json);

    }

    public static void analyzer(String value,MyHbaseUtil hbaseUtil,Producer<String, String> producer,
                                String kafkaSend,EasyParser easyParser){

        String time;
        String content;
        String title;

        try{

            JSONObject job = JSON.parseObject(value);
            String platform = job.getString("platform");
            String tableName = job.getString("hbase_table_name");
            String url = job.getString("hbase_rowkey");

            if(table == null || !table.getName().getNameAsString().equals(tableName)){
                table = hbaseUtil.getTable(tableName);
            }

            Map<String,String> map = hbaseUtil.query(table, url);

            time = map.get("time");
            content = map.get("content");
            title = map.get("title").replaceFirst("\"","“");
            title = title.replaceFirst("\"","”");
            title = title.replaceFirst("\"","“");
            title = title.replaceFirst("\"","”");

            String summary = "";

            if (!content.equals("")) {

                try {
                    summary = easyParser.getSummary(title,content);
                } catch(Exception e){

                    e.printStackTrace();
                    System.out.println("提取摘要异常");

                }
            }

            // 行业
            String industry = Arrays.toString(easyParser.parseNews(3, content, title));

            // 概念
            String section = Arrays.toString(easyParser.parseNews(2, content, title));

            // 股票
            String stock = Arrays.toString(easyParser.parseNews(1, content, title));

            int newsType = 0;
            int platformId = Integer.valueOf(platform);

            if (platformId > 10000 && platformId < 20000)
                newsType = 1; //快讯
            else if (platformId > 40000 && platformId < 50000)
                newsType = 2; //达人观点
            else if (platformId > 60000 && platformId < 70000)
                newsType = 3; //研报
            else if (platformId > 50000 && platformId < 60000)
                newsType = 4; //公告

            String[] date = getTime(time);



            String otherInfo = newsType + "<=" + title + "<=" + summary + "<=" + getPlatformName(platformId) +
                        "<=" + date[0] + "<=" +date[1] + "<=" + industry + "<=" + section + "<=" + stock + "<=" +
                        content; //+ "<=" +"标签"
            //System.out.println("otherInfo: " + otherInfo);

            scala.collection.immutable.List<String> newTitle = SegmentHan.segment(title, false);
            producer.send(new ProducerRecord<String,String>(kafkaSend,"",url + "=>" + newTitle + "=>" + otherInfo));


        }catch (Exception e){
            e.printStackTrace();
        }

    }


    public static String[] getTime(String time){

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HHmmss");
        Date date = new Date(Long.valueOf(time));
        String newDate = sdf.format(date);
        return newDate.split(" ");

    }

    public static String getPlatformName(int id){

        switch (id){
            case 1:
                return "同花顺股票";
            case 2:
                return "知乎";
            case 3:
                return "第一财经";
            case 4:
                return "微博";
            case 5:
                return "21CN";
            case 6:
                return "同花顺新闻";
            case 7:
                return "雪球";
            case 8:
                return "大智慧";
            case 9:
                return "东方财富";
            case 10:
                return "政府网";
            case 11:
                return "全景网";
            case 12:
                return "和讯";
            case 13:
                return "证券之星";
            case 14:
                return "财经网";
            case 15:
                return "金融界";
            case 16:
                return "中国财经信息网";
            case 17:
                return "中证网";
            case 18:
                return "上海证券报";
            case 19:
                return "证券时报网·中国";
            case 20:
                return "新华网财经";
            case 21:
                return "凤凰财经";
            case 22:
                return "新浪财经";
            case 23:
                return "搜狐财经";
            case 24:
                return "网易财经";
            case 25:
                return "华尔街见闻";
            case 26:
                return "腾讯财经";
            case 27:
                return "中国网";
            case 28:
                return "国际金融报";
            case 29:
                return "环球老虎网";
            case 30:
                return "优品财富";
            case 31:
                return "智通财经网";
            case 32:
                return "中青网";
            case 10001:
                return "财联社新闻";
            case 30004:
                return "金贝塔";
            case 30006:
                return "微信";
            case 50001:
                return "上海证券交易所";
            case 50002:
                return "深圳证券交易所";
            case 50003:
                return "巨潮资讯";
            case 60001:
                return "爱研报";
            case 60012:
                return "和讯研报";
            case 60013:
                return "东方财富研报";
            default:
                return "默认来源";
        }

    }


}
