package com.kunyan;

import com.kunyan.entity.News;
import com.kunyan.util.ElasticUtil;
import com.kunyan.util.MyHbaseUtil;
import com.kunyan.util.MySqlUtil;
import com.nlp.util.EasyParser;
import com.nlp.util.SegmentHan;
import de.mwvb.base.xml.XMLDocument;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by Administrator on 2017/9/6.
 * <p>
 * 运行主类
 */
public class Scheduler {

    private static Table table = null;

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

            String mysqlStockUrl = "jdbc:mysql://192.168.1.113/news?user=stock&password=stock&useUnicode=true&characterEncoding=utf8";
            final EasyParser easyParser = EasyParser.apply(mysqlStockUrl);


            XMLDocument doc = new XMLDocument();
            doc.loadFile(args[0]);
            final long timeStar = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]).getTime();
            final long timeEnd = new SimpleDateFormat("yyyy-MM-dd").parse(args[2]).getTime();

            final ElasticUtil elasticUtil = new ElasticUtil(doc);

            String groupId = doc.selectSingleNode("xml/kafka/groupId").getText();
            //String newsReceive = doc.selectSingleNode("xml/kafka/newsreceive").getText();
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
            deleteEsData(args[1], args[2], elasticUtil);

            new Thread(new Runnable() {
                public void run() {

                    //扫描hbase数据写入kafka
                    getData(timeStar, timeEnd, easyParser, producer, sentimentSend, resultSet, elasticUtil, hbaseUtil);
                }
            }).start();

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
            boolean related;
            List<String> remark;
            String tags;
            String timeSpider;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {

                    //System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                    value = record.value();

                    arr = value.split("<=");
                    if (arr.length == 18) {

                        newsType = arr[4];
                        newsTitle = arr[5];
                        newsSummary = arr[6];
                        site = arr[7];
                        newsUrl = arr[0];
                        newsDate = arr[8];
                        newsTime = arr[9];
                        industries = getList(arr[10]);
                        sections = getList(arr[11]);
                        stocks = getList(arr[12]);
                        positiveRate = Float.valueOf(arr[1]);
                        neutralRate = Float.valueOf(arr[2]);
                        passiveRate = Float.valueOf(arr[3]);
                        newsBody = arr[13];
                        related = arr[14].contains("y");
                        remark = getList(arr[15]);
                        tags = arr[16];
                        timeSpider = arr[17];

                        News news = new News(newsType, newsTitle, newsSummary,
                                site, newsUrl, newsDate,
                                newsTime, industries, sections,
                                stocks, positiveRate, neutralRate,
                                passiveRate, newsBody, related, remark, tags, newsUrl, timeSpider);
                        insertES(news, elasticUtil);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteEsData(String timeStart, String timeEnd, ElasticUtil elasticUtil) throws ExecutionException, InterruptedException {
        Set<String> ids = elasticUtil.searchIds(timeStart, timeEnd);
        for (String id : ids) {
            elasticUtil.deleteById(id);
        }
    }

    public static void getData(long timeStar, long timeEnd, EasyParser easyParser, Producer producer, String kafkaSend,
                               ResultSet resultSet, ElasticUtil elasticUtil, MyHbaseUtil myHbaseUtil) {

        Table table1 = myHbaseUtil.getTable("news_detail");
        Table table2 = myHbaseUtil.getTable("new_news");
        Table[] tables = new Table[]{table1, table2};

        for (Table t : tables) {
            Scan scan = new Scan();
            try {
                scan.setTimeRange(timeStar, timeEnd);
                scan.setCaching(2000);
                ResultScanner results = t.getScanner(scan);
                Result result = results.next();

                int newsType = 0;
                Pattern pattern = Pattern.compile("\\d+年\\d+月\\d+日 \\d+:\\d+:\\d+");
                Matcher matcher;

                String articleType;
                String content;
                String platform;
                String related;
                String tags;
                String remark;
                String time;
                String timeSpider;
                String title;
                String url;
                String summary = "";

                String industry = "";//行业
                String section = "";//板块
                String stock = "";//股票

                String[] date;
                while (null != result) {
                    try {
                        result = results.next();
                        byte[] bPlatform = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("platform"));
                        if (bPlatform == null) bPlatform = new byte[]{};
                        platform = Bytes.toString(bPlatform);
                        int platformInt = Integer.valueOf(platform);

                        //去掉雪球的大V文章
                        if (!platform.equals("60005")) {
                            byte[] bTitle = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"));
                            if (bTitle == null) bTitle = new byte[]{};
                            title = Bytes.toString(bTitle).replaceFirst("\"", "“");
                            title = title.replaceFirst("\"", "”");
                            title = title.replaceFirst("\"", "“");
                            title = title.replaceFirst("\"", "”");


                            byte[] bArticleType = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("article_type"));
                            byte[] bContent = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"));
                            byte[] bRelated = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("related"));
                            byte[] bTags = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("tags"));
                            byte[] bTime = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("time"));
                            byte[] bTimeSpider = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("time_spider"));
                            byte[] bUrl = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("url"));
                            byte[] bRemark = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("remark"));

                            if (bArticleType == null) bArticleType = new byte[]{};
                            if (bContent == null) bContent = new byte[]{};
                            if (bRelated == null) bRelated = new byte[]{};
                            if (bTags == null) bTags = new byte[]{};
                            if (bTime == null) bTime = new byte[]{};
                            if (bTimeSpider == null) bTimeSpider = new byte[]{};
                            if (bUrl == null) bUrl = new byte[]{};
                            if (bRemark == null) bRemark = new byte[]{};

                            articleType = Bytes.toString(bArticleType);
                            content = Bytes.toString(bContent);
                            related = Bytes.toString(bRelated);
                            tags = Bytes.toString(bTags);
                            time = Bytes.toString(bTime);
                            timeSpider = Bytes.toString(bTimeSpider);
                            if (time.equals("")) {
                                time = timeSpider;
                            }
                            if(time.length() == 10){
                                time = time + "000";
                            }
                            if(timeSpider.length() == 10){
                                timeSpider = timeSpider + "000";
                            }

                            url = Bytes.toString(bUrl);
                            remark = Bytes.toString(bRemark);

                            if (platformInt == 60003 && !content.equals("")) {
                                matcher = pattern.matcher(content);

                                if (matcher.find()) {
                                    String str = matcher.group(0);
                                    content = content.replaceFirst(str, "").replaceFirst("\\n", "");
                                }
                            }

                            if (!content.equals("")) {
                                try {
                                    summary = easyParser.getSummary(title, content);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            } else {
                                summary = "";
                            }
                            summary = summary.replaceAll("\\r", "")
                                    .replaceAll("\\n", "");

                            if (!articleType.equals("")) {

                                String[] remarks = ",".split(remark);

                                for (String r : remarks) {

                                    if (r.equals("行业新闻来源")) {
                                        industry = Arrays.toString(easyParser.parseNews(3, content, title));
                                    } else if (r.equals("板块概念的新闻来源")) {
                                        section = Arrays.toString(easyParser.parseNews(2, content, title));
                                    } else if (r.equals("个股新闻来源")) {
                                        stock = Arrays.toString(easyParser.parseNews(1, content, title));
                                    }
                                }

                                if (articleType.equals("新闻")) {
                                    newsType = 0;
                                } else if (articleType.equals("快讯")) {
                                    newsType = 1;
                                } else if (articleType.equals("达人观点")) {
                                    newsType = 2;
                                } else if (articleType.equals("研报")) {
                                    newsType = 3;
                                } else if (articleType.equals("公告")) {
                                    newsType = 4;
                                } else if (articleType.equals("行情分析")) {
                                    newsType = 5;
                                } else if (articleType.equals("行情图表")) {
                                    newsType = 6;
                                }
                            } else {
                                industry = "";
                                section = "";
                                stock = "";

                                if (platformInt >= 10000 && platformInt <= 20000) {
                                    newsType = 1; //快讯
                                } else if (platformInt >= 50000 && platformInt <= 60000) {
                                    newsType = 4;//公告
                                } else if (platformInt >= 40000 && platformInt <= 50000) {
                                    newsType = 2;
                                } else if (platformInt == 60007 || platformInt == 60003) {
                                    newsType = 2;
                                } else if (platformInt == 60001 || platformInt == 60012 || platformInt == 60013) {
                                    newsType = 3;
                                }
                            }

                            //标题去重
                            if (!esTitleExist(title, elasticUtil,newsType)) {
                                date = getTime(time).replaceAll(":", "").split(" ");
                                timeSpider = getTime(timeSpider);
                                String otherInfo = newsType + "<=" + title + "<=" + summary + "<=" + getPlatformName(platformInt) +
                                        "<=" + date[0] + "<=" + date[1] + "<=" + industry + "<=" + section + "<=" + stock + "<=" +
                                        content + "<=" + related + "<=" + remark + "<=" + tags + "<=" + timeSpider;

                                scala.collection.immutable.List<String> newTitle = SegmentHan.segment(title, false);
                                producer.send(new ProducerRecord<String, String>(kafkaSend, "", url + "=>" + newTitle + "=>" + otherInfo));
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("hbase数据取完");

        try {
            if (resultSet != null) {
                String articleType;
                String content;
                String platform;
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

                    //标题去重
                    if (!esTitleExist(title, elasticUtil,1)) {
                        url = resultSet.getString("url");
                        summary = resultSet.getString("summary");
                        summary = summary.replaceAll("\\r", "")
                                .replaceAll("\\n", "");

                        time = resultSet.getString("news_time");
                        timeSpider = resultSet.getString("updated_time");

                        if(time.length() == 10){ time = time + "000"; }
                        if(timeSpider.length() == 10){ timeSpider = timeSpider + "000"; }

                        date = Scheduler.getTime(time).replaceAll(":", "").split(" ");
                        content = resultSet.getString("content");

                        //快讯无正文
                        String otherInfo = 1 + "<=" + title + "<=" + summary + "<=" + resultSet.getString("source") +
                                "<=" + date[0] + "<=" + date[1] + "<=" + industry + "<=" + section + "<=" + stock + "<=" +
                                content + "<=" + related + "<=" + remark + "<=" + tags + getTime(timeSpider);

                        scala.collection.immutable.List<String> newTitle = SegmentHan.segment(title, false);
                        producer.send(new ProducerRecord<String, String>(kafkaSend, "", url + "=>" + newTitle + "=>" + otherInfo));

                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static List<String> getList(String str) {

        List<String> list = new ArrayList<String>();
        String[] arr = str.replace("[", "").replace("]", "").split(",");
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
        if(newsType == 4 && differ == 0){
            return true;
        }else{
            return !likeTitle.equals("") && differ <= 5 && differ >= -5;
        }

    }


}
