package com.kunyan.thread;

import com.nlp.util.SegmentHan;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.kunyan.Scheduler.*;

public class DisposeDataThread implements Runnable {

    private Result result;
    private String kafkaSend;
    private Producer producer;

    public DisposeDataThread(Result result, String kafkaSend, Producer producer) {
        this.result = result;
        this.kafkaSend = kafkaSend;
        this.producer = producer;
    }

    public void run() {
        int newsType = 0;

        String articleType;
        String content;
        String platform;
        String related;
        String tags;
        String remark;
        String time = "";
        String timeSpider;
        String title;
        String url = "";
        String summary = "";

        String industry = "";//行业
        String section = "";//板块
        String stock = "";//股票

        String[] date;
        String rowKey;
        long timeStamp ;
        try {
            byte[] bPlatform = result.getValue(Bytes.toBytes("basic"), Bytes.toBytes("platform"));
            if (bPlatform == null) bPlatform = new byte[]{};
            platform = Bytes.toString(bPlatform);

            rowKey = Bytes.toString(result.getRow());
            System.out.println("rowkey " + rowKey);
            timeStamp =result.rawCells()[0].getTimestamp();

            if(!platform.equals("") && !platform.equals("0")){
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

                    if(!tags.equals("公司详情")){
                        time = Bytes.toString(bTime);
                        timeSpider = Bytes.toString(bTimeSpider);
                        if (time.equals("") || time.equals("0")) {
                            time = timeSpider;
                        }
                        if (time.length() == 10) {
                            time = time + "000";
                        }
                        if (timeSpider.length() == 10) {
                            timeSpider = timeSpider + "000";
                        }

                        url = Bytes.toString(bUrl);
                        if(url.equals("")){
                            url = rowKey;
                        }
                        remark = Bytes.toString(bRemark);

                        if (platformInt == 60003 && !content.equals("")) {
                            String str = getTime(time);
                            content = content.replaceFirst(str, "").replaceFirst("\\n", "");
                        }

                        if (!content.equals("")) {
                            try {
                                summary = easyParser.getSummary(title, content);
                            }catch (Exception e){
                                e.printStackTrace();
                            }catch (Error e) {
                                e.printStackTrace();
                                System.out.println("摘要提取错误 " + url);
                            }
                        }
                        summary = summary.replaceAll("\\r", "")
                                .replaceAll("\\n", "");

                        if (!remark.equals("")) {

                            String[] remarks = remark.split(",");

                            for (String r : remarks) {

                                if (r.equals("行业新闻来源")) {
                                    industry = Arrays.toString(easyParser.parseNews(3, title, content));
                                    System.out.println("行业 " + stock + "\t"+url);
                                } else if (r.contains("板块") || r.contains("概念")) {
                                    section = Arrays.toString(easyParser.parseNews(2, title, content));
                                    System.out.println("概念 " + stock + "\t"+url);
                                } else if (r.equals("个股新闻来源")) {
                                    stock = Arrays.toString(easyParser.parseNews(1, title, content));
                                    System.out.println("个股 " + stock + "\t"+url);
                                }
                            }

                            articleType = articleType.split(",")[0];
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
                        if (!esTitleExist(title, elasticUtil, newsType)) {
                            if(time.startsWith("[")){
                                System.out.println("错误时间 " + url);
                                date = time.replaceAll("\\[","").replaceAll("]","").trim().split(" ");
                            }else{
                                if(time.startsWith("1") && time.length() == 13){
                                    date = getTime(time).replaceAll(":", "").split(" ");
                                }else{
                                    date = getTime(timeStamp + "").replaceAll(":", "").split(" ");
                                }
                            }

                            if(!timeSpider.equals("") && timeSpider.startsWith("1") && timeSpider.length() == 13){
                                timeSpider = getTime(timeSpider);
                            }else{
                                timeSpider = getTime(timeStamp + "");
                            }
                            String otherInfo = newsType + "<=" + title + "<=" + summary + "<=" + getPlatformName(platformInt) +
                                    "<=" + date[0] + "<=" + date[1] + "<=" + industry + "<=" + section + "<=" + stock + "<=" +
                                    content + "<=" + related + "<=" + remark + "<=" + tags + "<=" + timeSpider;

                            String newTitle = SegmentHan.segmentReturnString(title, false);
                            ProducerRecord<String, String> producerRecord =
                                    new ProducerRecord<String, String>(kafkaSend, "",url + "=>" + newTitle + "=>" + otherInfo);
                            producer.send(producerRecord);
                            System.out.println(url);
                        }
                    }
                }
            }

        } catch (NoSuchElementException noSuchElementException){
          noSuchElementException.printStackTrace();
        }catch (NumberFormatException numberFormatException){
            numberFormatException.printStackTrace();
            System.out.println("时间错误：" + url + " "+  time);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(url);
        }
    }

}
