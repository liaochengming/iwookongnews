package com.kunyan;

import com.kunyan.entity.News;
import com.kunyan.thread.DisposeDataThread;
import com.kunyan.thread.InputESThread;
import com.kunyan.util.*;
import com.nlp.util.EasyParser;
import com.nlp.util.SegmentHan;
import de.mwvb.base.xml.XMLDocument;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by Administrator on 2017/9/6.
 * <p>
 * 运行主类
 */
public class Scheduler {

    public static EasyParser easyParser;
    public static ElasticUtil elasticUtil;

    /**
     * 主方法
     *
     * @param args 1是配置文件路径 2是导数据的开始日期 3是导数据的结束日期 日期格式是yyyy-MM-dd
     */
    public static void main(final String[] args) {


        if (args.length < 3) {
            System.out.println("缺少参数");
            System.exit(0);
        }

        try {

            XMLDocument doc = new XMLDocument();
            doc.loadFile(args[0]);
            final long timeStar = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]).getTime();
            final long timeEnd = new SimpleDateFormat("yyyy-MM-dd").parse(args[2]).getTime();

            elasticUtil = new ElasticUtil(doc);

            String mysqlStockUrl = doc.selectSingleNode("xml/mysql/parseUrl").getText();
            String dictPath = doc.selectSingleNode("xml/path/custom_dict").getText();
            easyParser = EasyParser.apply(mysqlStockUrl,dictPath);

            String groupId = doc.selectSingleNode("xml/kafka/groupId").getText();
//            String newsReceive = doc.selectSingleNode("xml/kafka/newsreceive").getText();
            String sentimentBack = doc.selectSingleNode("xml/kafka/sentiment_back").getText();
            final String sentimentSend = doc.selectSingleNode("xml/kafka/sentiment_send").getText();
            String brokerList = doc.selectSingleNode("xml/kafka/brokerList").getText();

            String rootDir = doc.selectSingleNode("xml/hbase/rootDir").getText();
            String ip = doc.selectSingleNode("xml/hbase/ip").getText();
            final MyHbaseUtil hbaseUtil = new MyHbaseUtil(rootDir, ip);

            String mysqlUrl = doc.selectSingleNode("xml/mysql/url").getText();
            String userName = doc.selectSingleNode("xml/mysql/user_name").getText();
            String passWord = doc.selectSingleNode("xml/mysql/pass_word").getText();

            Connection conn = MySqlUtil.getMysqlConn(mysqlUrl, userName, passWord);
            final ResultSet resultSet = MySqlUtil.getMysqlData(conn, "select * from news_info where type=1 and news_time>" + timeStar + " and news_time<" + timeEnd);

            Properties kafkaConsumerProps = new Properties();
            kafkaConsumerProps.put("bootstrap.servers", brokerList);
            kafkaConsumerProps.put("group.id", groupId);
            kafkaConsumerProps.put("enable.auto.commit", "true");
            kafkaConsumerProps.put("auto.commit.interval.ms", "1000");
            kafkaConsumerProps.put("session.timeout.ms", "30000");
            kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            Properties kafkaProducerProps = new Properties();
            kafkaProducerProps.put("bootstrap.servers", brokerList);
            kafkaProducerProps.put("acks", "all");
            kafkaProducerProps.put("retries", 0);
            kafkaProducerProps.put("batch.size", 16384);
            kafkaProducerProps.put("linger.ms", 1);
            kafkaProducerProps.put("buffer.memory", 33554432);
            kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaConsumerProps);
            final Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProducerProps);


            consumer.subscribe(Collections.singletonList(sentimentBack));

            //删除ES的数据
            elasticUtil.searchDelete(args[1],args[2]);

            //扫描hbase数据写入kafka
            new Thread(new Runnable() {
                public void run() {
                    getData(timeStar, timeEnd, producer, sentimentSend, resultSet, elasticUtil, hbaseUtil);
                }
            }).start();

            String value;
            ExecutorService executorService = Executors.newFixedThreadPool(1);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    value = record.value();
                    executorService.execute(new InputESThread(value,elasticUtil));
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void getData(long timeStar, long timeEnd,  Producer producer, String kafkaSend,
                                ResultSet resultSet, ElasticUtil elasticUtil, MyHbaseUtil myHbaseUtil) {

        Table table1 = myHbaseUtil.getTable("news_detail");
        Table table2 = myHbaseUtil.getTable("new_news");
        Table[] tables = new Table[]{table1, table2};

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (Table t : tables) {
            Scan scan = new Scan();
            ResultScanner results = null;
            try {
                scan.setTimeRange(timeStar, timeEnd);
                scan.setCaching(100);
                scan.setMaxVersions();
                results = t.getScanner(scan);
                Result result = results.next();
                int i = 0;
                long t1 = System.currentTimeMillis();
                System.out.println("开始扫描" + t.getName());
                while (null != result) {
                    executorService.execute(new DisposeDataThread(result,kafkaSend,producer));
                    result = results.next();
                    i++;
                }

                System.out.println("扫描完成" + i);
                long t2 = System.currentTimeMillis();
                System.out.println("扫描耗时 " + (t2-t1));
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                if(results != null){
                    results.close();
                }
            }
        }
        System.out.println("hbase数据取完");

        try {
            if (resultSet != null) {
                String content="";
                String related = "";
                String tags = "";
                String remark = "";
                String time;
                String timeSpider;
                String title;
                String url;
                String summary;

                String industry = "";//行业
                String section = "";//板块
                String stock = "";//股票

                String[] date;

                while (resultSet.next()) {

                    title = resultSet.getString("title");

                    System.out.println("查询的mysql的数据:   " + resultSet.getString("url") );
                    //标题去重
                    if (!esTitleExist(title, elasticUtil, 1)) {
                        url = resultSet.getString("url");
                        summary = resultSet.getString("summary");
                        summary = summary.replaceAll("\\r", "")
                                .replaceAll("\\n", "");

                        time = resultSet.getString("news_time");
                        timeSpider = resultSet.getString("updated_time");

                        if (time.length() == 10) {
                            time = time + "000";
                        }
                        if (timeSpider.length() == 10) {
                            timeSpider = timeSpider + "000";
                        }

                        date = Scheduler.getTime(time).replaceAll(":", "").split(" ");
                        //content = resultSet.getString("content");

                        //快讯无正文
                        String otherInfo = 1 + "<=" + title + "<=" + summary + "<=" + resultSet.getString("source") +
                                "<=" + date[0] + "<=" + date[1] + "<=" + industry + "<=" + section + "<=" + stock + "<=" +
                                content + "<=" + related + "<=" + remark + "<=" + tags + "<=" +getTime(timeSpider);

                        scala.collection.immutable.List<String> newTitle = SegmentHan.segment(title, false);
                        producer.send(new ProducerRecord<String, String>(kafkaSend, "", url + "=>" + newTitle + "=>" + otherInfo));
                        System.out.println("发送的数据:   " + url );
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("mysql数据取完");
    }


    public static List<String> getList(String str) {

        List<String> list = new ArrayList<String>();
        String[] arr = str.replace("[", "").replace("]", "").split(",");
        if(arr[0].equals("") || arr[0].equals("null")){
            return null;
        }
        Collections.addAll(list, arr);
        return list;
    }


    public static void insertES(News news, ElasticUtil elasticUtil) {

        String json = elasticUtil.CreateNews(news);
        elasticUtil.createIndex(json);

    }

    public static String getTime(String time) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(Long.valueOf(time));
        return sdf.format(date);

    }

    public static String getPlatformName(int id) {

        switch (id) {
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
            case 40001:
                return "雪球";
            case 40002:
                return "中金博客";
            case 40003:
                return "摩尔金融";
            case 50001:
                return "上海证券交易所";
            case 50002:
                return "深圳证券交易所";
            case 50003:
                return "巨潮资讯";
            case 60001:
                return "爱研报";
            case 60003:
                return "摩尔金融";
            case 60005:
                return "雪球";
            case 60007:
                return "中金博客";
            case 60012:
                return "和讯研报";
            case 60013:
                return "东方财富研报";
            default:
                return "默认来源";
        }

    }

    public static boolean esTitleExist(String title, ElasticUtil elasticUtil, int newsType) throws ParseException {

        String likeTitle = elasticUtil.hasFieldLike("title", title, "100%");
        int likeTitleLength = likeTitle.length();
        int titleLength = title.length();
        int differ = likeTitleLength - titleLength;
        return newsType == 4 && differ == 0 || !likeTitle.equals("") && differ <= 5 && differ >= -5;

    }

    public static boolean esContentExist(String title, String content, ElasticUtil elasticUtil, String url) {
        String contentBeg;

        if (content.length() > 100) {
            contentBeg = content.substring(0, 100);
        } else {
            contentBeg = content;
        }

        SearchHits searchHits = elasticUtil.searchSomeLike("title", title, "80%");
        String esContent;

        for (SearchHit searchHit : searchHits) {
            esContent = (String) searchHit.getSource().get("body");
            if (esContent.startsWith(contentBeg)) {
                System.out.println("新闻正文相似  新标题为: " + title + "\t" + "新url: " + url);
                System.out.println("ES标题为: " + searchHit.getSource().get("title") +
                        "ES url为: " + searchHit.getSource().get("url"));
                return true;
            }
        }

        return false;
    }


}
