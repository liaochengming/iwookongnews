package com.kunyan.thread;

import com.kunyan.entity.News;
import com.kunyan.util.ElasticUtil;

import java.text.ParseException;
import java.util.List;

import static com.kunyan.Scheduler.*;

public class InputESThread implements Runnable {


    private String data;
    private ElasticUtil elasticUtil;

    public InputESThread(String data, ElasticUtil elasticUtil){
        this.data = data;
        this.elasticUtil = elasticUtil;
    }

    public void run() {
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
        List<String> showcase;

        arr = data.split("<=");
        if (arr.length == 19) {

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
            showcase = getList(arr[18]);

            News news = new News(newsType, newsTitle, newsSummary,
                    site, newsUrl, newsDate,
                    newsTime, industries, sections,
                    stocks, positiveRate, neutralRate,
                    passiveRate, newsBody, related,
                    remark, tags, newsUrl,
                    timeSpider,showcase);
            try {
                if (!esTitleExist(newsTitle, elasticUtil, Integer.valueOf(newsType))) {
                    if (!esContentExist(newsTitle, newsBody, elasticUtil, newsUrl)) {
                        long t1 = System.currentTimeMillis();
                        insertES(news,elasticUtil);
                        long t2 = System.currentTimeMillis();
                        System.out.println("写入数据:title " + newsTitle + "\t" + "url: " + newsUrl);
                        System.out.println("写入ES耗时: " + (t2 - t1) + "ms");
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
            System.out.println(arr[15] + " " +newsUrl);
        }
    }
}
