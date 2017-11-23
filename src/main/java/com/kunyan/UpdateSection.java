package com.kunyan;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class UpdateSection {

    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        String index = doc.selectSingleNode("xml/es/index").getText();
        String type = doc.selectSingleNode("xml/es/type").getText();
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        SearchResponse response = elasticUtil.client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(1000)
                .setFetchSource(new String[]{"remarks"},new String[]{})
                .setQuery(QueryBuilders.termsQuery("remarks","板块概念新闻来源","概念新闻来源","概念板块新闻来源","板块概念的新闻来源"))
                .get();
        SearchHits searchHits = response.getHits();
        System.out.println(searchHits.totalHits);
        for(SearchHit searchHit :searchHits){

            System.out.println(searchHit.getSource().get("remarks"));
            ArrayList<String> remarks = (ArrayList<String>) searchHit.getSource().get("remarks");
            if(remarks.contains("板块概念新闻来源")){
                remarks.remove("板块概念新闻来源");
            }else if(remarks.contains("概念新闻来源")){
                remarks.remove("概念新闻来源");
            }else if(remarks.contains("概念板块新闻来源")){
                remarks.remove("概念板块新闻来源");
            }else if(remarks.contains("板块概念的新闻来源")){
                remarks.remove("板块概念的新闻来源");
            }
            remarks.add("概念板块新闻来源");
            String id = searchHit.getId();
            JSONObject job = new JSONObject();
            job.put("remarks", remarks);
            elasticUtil.updateData(id, job.toString());

        }
    }
}
