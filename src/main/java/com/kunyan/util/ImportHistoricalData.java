package com.kunyan.util;

import com.kunyan.Scheduler;
import com.kunyan.entity.News;
import de.mwvb.base.xml.XMLDocument;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/9/11.
 * 从数据库导入相关新闻数据到elasticsearch
 */
public class ImportHistoricalData {

    static String type;
    static String newsUrl;
    static String summary;
    static String newsDate;
    static String newsTime;
    static String[] date;
    static String content="";
    static News news;
    static String showcase = "";
    static ElasticUtil elasticUtil;


    public static void main(String[] args) {

        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        elasticUtil = new ElasticUtil(doc);
        String mysqlUrl = doc.selectSingleNode("xml/mysql/url").getText();
        String userName = doc.selectSingleNode("xml/mysql/user_name").getText();
        String passWord = doc.selectSingleNode("xml/mysql/pass_word").getText();

        MyHbaseUtil myHbaseUtil = new MyHbaseUtil("hdfs://pmaster:9000/hbase","pslave1,pslave2,pslave3");
//        MyHbaseUtil myHbaseUtil = new MyHbaseUtil("hdfs://master1:9000/hbase","slave1,slave2,slave3");
        Table table1 = myHbaseUtil.getTable("news_detail");
        Table table2 = myHbaseUtil.getTable("new_news");

        Connection conn = MySqlUtil.getMysqlConn(mysqlUrl, userName, passWord);

        ResultSet resultSet = MySqlUtil.getMysqlData(conn, "select * from news_info where news_time>1502812800000");


        DecimalFormat df = (DecimalFormat)DecimalFormat.getInstance();
        df.setGroupingUsed(false);
        df.setMaximumFractionDigits(6);

        Pattern pattern= Pattern.compile("\\d+年\\d+月\\d+日 \\d+:\\d+:\\d+");
        Matcher matcher;

        Get get;

        try {
            if(resultSet != null){

                while (resultSet.next()){

                    type = resultSet.getString("type");
                    newsUrl = resultSet.getString("url");
                    summary = resultSet.getString("summary");
                    date = Scheduler.getTime(resultSet.getString("news_time")).replaceAll(" ","").split(" ");
                    newsDate = date[0];
                    newsTime = date[1];
                    showcase = resultSet.getString("showcase");

                    //快讯无正文
                    if(!type.equals("1")){

                        get = new Get(newsUrl.getBytes());
                        content = queryHbase(table1,get);
                        if(content.equals("")){
                            content = queryHbase(table2,get);
                        }
                    }else{
                        content = "";
                    }

                    //去掉bigv的摩尔正文和摘要开头的时间
                    if(type.equals("2") && !content.equals("")){
                        matcher = pattern.matcher(content);

                        if(matcher.find()){
                            String str = matcher.group(0);
                            content = content.replaceFirst(str,"").replaceFirst("\\n","");
                            summary = summary.replaceFirst(str,"").replaceFirst("\\n","");
                        }
                    }


                    news =  new News(type,
                            resultSet.getString("title"),
                            summary,
                            resultSet.getString("source"),
                            resultSet.getString("url"),
                            newsDate,
                            newsTime,
                            Scheduler.getList(resultSet.getString("industry")),
                            Scheduler.getList(resultSet.getString("section")),
                            Scheduler.getList(resultSet.getString("stock")),
                            resultSet.getFloat("positive_rate"),
                            resultSet.getFloat("neutral_rate"),
                            resultSet.getFloat("passive_rate"),
                            content,
                            true,new ArrayList<String>(),
                            "",
                            newsUrl,
                            Scheduler.getTime(resultSet.getString("updated_time")),
                            Scheduler.getList(showcase));
                    Scheduler.insertES(news, elasticUtil);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static String queryHbase(Table table,Get get){

        try {
            byte[] bytes = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"));
            if (bytes == null) bytes = new byte[0];
            content =   new String(bytes, "UTF-8");
            return content;
        } catch (IOException e) {
            e.printStackTrace();
            return  content;
        }
    }

}
