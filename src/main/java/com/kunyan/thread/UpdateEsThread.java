package com.kunyan.thread;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.kunyan.Scheduler;
import com.kunyan.conf.Platform;
import com.kunyan.util.ElasticUtil;
import com.kunyan.util.MyHbaseUtil;
import com.nlp.EasyParser;
import com.nlp.util.SegmentHan;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.*;

public class UpdateEsThread implements Runnable{


    private String value;
    private ElasticUtil elasticUtil;
    private MyHbaseUtil hbaseUtil;
    private EasyParser easyParser;
    private long offset;

    public UpdateEsThread(String value, ElasticUtil elasticUtil, MyHbaseUtil hbaseUtil,
                          EasyParser easyParser, long offset){
        this.value = value;
        this.elasticUtil = elasticUtil;
        this.hbaseUtil = hbaseUtil;
        this.easyParser = easyParser;
        this.offset = offset;
    }

    public void run() {

        Table table = hbaseUtil.getTable("new_news");
        JSONObject job = JSON.parseObject(value);
        String url = job.getString("hbase_rowkey");
        String update = job.getString("update");
        String platform = job.getString("platform");
        int platformId = Integer.valueOf(platform);

        if(update!=null && update.equals("true")){

            long t1 = System.currentTimeMillis();

            Map<String,String> map = query(table,url);

            long t2 = System.currentTimeMillis();
            System.out.println("hbase耗时：" + (t2-t1));

            upDateES(map,url,platformId);

            long t3 = System.currentTimeMillis();
            System.out.println("更新es耗时：" + (t3-t2));
        }

    }

    public void upDateES(Map<String,String> map, String url,int platformId) {

        assert map != null;
        String content = map.get("content");
        String title = map.get("title").replaceFirst("\"", "“");
        title = title.replaceFirst("\"", "”");
        title = title.replaceFirst("\"", "“");
        title = title.replaceFirst("\"", "”");
        String related = map.get("related");
        String remark = map.get("remark");
        String tags = map.get("tags");
        String timeSpider = map.get("timeSpider");
        String time = map.get("time");
        String articleType = map.get("articleType");
        String tagsManual = map.get("tagsManual");

        if (time.length() == 10) {
            time = time + "000";
        }
        if (timeSpider.length() == 10) {
            timeSpider = timeSpider + "000";
        }
        timeSpider = longTimeToString(Long.valueOf(timeSpider));

        String[] date = longTimeToString(Long.valueOf(time))
                .replaceAll(":", "").split(" ");
        // 行业
        String industry = "";
        // 概念
        String section = "";
        // 股票
        String stock = "";

        String[] remarks = remark.split(",");

        for (String r : remarks) {

            if (r.equals("行业新闻来源")) {
                industry = Arrays.toString(easyParser.parseNews(3, title, content));

            } else if (r.contains("板块") || r.contains("概念")) {
                section = Arrays.toString(easyParser.parseNews(2, title, content));
                remark = remark.replace(r,"概念板块新闻来源");

            } else if (r.equals("个股新闻来源")) {
                stock = Arrays.toString(easyParser.parseNews(1, title, content));

            }
        }

        boolean isRelated = related.contains("y");
        List<String> remarkList = Scheduler.getList(remark);
        JSONObject job = new JSONObject();
        job.put("industries", Scheduler.getList(industry));
        job.put("sections", Scheduler.getList(section));
        job.put("stocks", Scheduler.getList(stock));
        job.put("related", isRelated);
        job.put("remarks", remarkList);
        job.put("tags", tags);
        job.put("news_date", date[0]);
        job.put("news_time", date[1]);
        job.put("time_spider", timeSpider);
        String platformStr = "";
        for (Platform p : Platform.values()) {
            if (p.getNum() == platformId) {
                platformStr = p.getName();
            }
        }
        job.put("site",platformStr);
        job.put("body",content);
        job.put("title",title);
        String summary = "";

        if (!content.equals("")) {

            try {
                summary = SegmentHan.getSummary(title, content);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        summary = summary.replaceAll("\\r", "")
                .replaceAll("\\n", "");
        job.put("summary",summary);


        articleType = articleType.split(",")[0];
        int newsType = 0;

        if(articleType.contains(",")){
            articleType = articleType.split(",")[0];
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

        job.put("type",String.valueOf(newsType));
        job.put("tags_manual", Scheduler.getList(tagsManual));
        System.out.println("更新操作 - offset: [ " +offset+ " ]\t" + " tags_manual: [ " + tagsManual + " ]\t" + "title: [ " + title + " ]");

        try {
            String id = elasticUtil.selectId(url);
            if (!id.equals("")) {
                elasticUtil.updateData(id, JSONObject.toJSONString(job, SerializerFeature.WriteMapNullValue));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public Map<String, String> query(Table table, String rowKey) {

        Map<String, String> map = new HashMap<String, String>();
        Get get = new Get(rowKey.getBytes());

        try {

            byte[] content = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("content"));
            byte[] title = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("title"));
            if (content == null && title == null) {
                //logger.info("Get empty data by this rowkey:" + rowKey);
                return null;
            }
            byte[] related = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("related"));
            byte[] remark = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("remark"));
            byte[] tags = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("tags"));
            byte[] timeSpider = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("time_spider"));
            byte[] time = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("time"));
            byte[] articleType = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("article_type"));
            byte[] showcase = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("showcase"));
            byte[] tagsManual = table.get(get).getValue(Bytes.toBytes("basic"), Bytes.toBytes("tags_manual"));

            if (related == null) related = new byte[0];
            if (remark == null) remark = new byte[0];
            if (tags == null) tags = new byte[0];
            if (content == null) content = new byte[0];
            if (title == null) title = new byte[0];
            if (timeSpider == null) timeSpider = new byte[0];
            if (time == null) time = new byte[0];
            if (articleType == null) articleType = new byte[0];
            if (showcase == null) showcase = new byte[0];
            if (tagsManual == null) tagsManual = new byte[0];

            map.put("content", new String(content, "UTF-8"));
            map.put("title", new String(title, "UTF-8"));
            map.put("related", new String(related, "UTF-8"));
            map.put("remark", new String(remark, "UTF-8"));
            map.put("tags", new String(tags, "UTF-8"));
            map.put("showcase", new String(showcase, "UTF-8"));
            map.put("tagsManual", new String(tagsManual, "UTF-8"));

            if (new String(time, "UTF-8").equals("") || new String(time, "UTF-8").equals("0")) {
                map.put("time", new String(timeSpider, "UTF-8"));
            } else {
                map.put("time", new String(time, "UTF-8"));
            }
            map.put("timeSpider", new String(timeSpider, "UTF-8"));
            map.put("articleType", new String(articleType, "UTF-8"));

            return map;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public  String longTimeToString(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(time);
        return sdf.format(date);
    }
}
