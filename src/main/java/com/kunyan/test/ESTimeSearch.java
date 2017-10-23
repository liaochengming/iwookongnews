package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.kunyan.Scheduler.easyParser;

public class ESTimeSearch {


    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile("src/main/resource/config.xml");
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        try {
            Set<SearchHit> searchHits = elasticUtil.timeSearchData("2017-09-01 00:00:00","2017-10-01 00:00:00");
            for(SearchHit hit : searchHits){
                ArrayList<String> remark;
                remark = (ArrayList<String>)hit.getSource().get("remarks");
                if(remark.size() != 0 && !remark.get(0).equals("")){
                    System.out.println(hit.getSource().get("title"));
                    if (remark.get(0).equals("行业新闻来源")) {
                        System.out.println("行业 " + hit.getSource().get("industries") + "\t" + hit.getSource().get("time_spider"));
                    } else if (remark.get(0).contains("板块") || remark.get(0).contains("概念")) {
                        System.out.println("概念 " + hit.getSource().get("industries") + "\t" + hit.getSource().get("time_spider"));
                    } else if (remark.get(0).equals("个股新闻来源")) {
                        System.out.println("个股 " + hit.getSource().get("stocks") + "\t" + hit.getSource().get("time_spider"));
                    }
                    System.out.println("===================================================");
                }
            }
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
