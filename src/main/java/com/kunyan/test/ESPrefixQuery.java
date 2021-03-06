package com.kunyan.test;

import com.alibaba.fastjson.JSONObject;
import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ESPrefixQuery {

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
                .setFetchSource(new String[]{"title"},new String[]{})
                .setScroll(TimeValue.timeValueMinutes(8))
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        SearchHits searchHits = response.getHits();
        System.out.println(searchHits.totalHits);
        Map<String,String> mapId = new HashMap<String, String>();
        for(SearchHit searchHit :searchHits){
            Object t = searchHit.getSource().get("title");
//            if(t instanceof Integer){
//                mapId.put(searchHit.getId(),t.toString());
//            }
            if(t.equals("")){
                System.out.println(searchHit.getId());
            }
        }
        String scrollId = response.getScrollId();
        int size = searchHits.getHits().length;
        while(size != 0){
            response = elasticUtil.client.prepareSearchScroll(scrollId)
                    .setScroll(TimeValue.timeValueMinutes(8)).get();
            searchHits = response.getHits();
            for(SearchHit searchHit : searchHits){
                //do....
                Object t = searchHit.getSource().get("type");
                if(t instanceof Integer){
                    mapId.put(searchHit.getId(),t.toString());
                }
            }
            scrollId = response.getScrollId();
            size = searchHits.getHits().length;
        }

        System.out.println("set size: " + mapId.size());
        for(String id :mapId.keySet()){
            JSONObject job = new JSONObject();
            job.put("type",mapId.get(id));
            elasticUtil.updateData(id, job.toString());
        }
    }
}
