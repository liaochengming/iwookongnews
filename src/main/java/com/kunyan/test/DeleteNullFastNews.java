package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.HashSet;
import java.util.Set;

public class DeleteNullFastNews {

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
                .setFetchSource(new String[]{"title","body"},new String[]{})
                .setScroll(TimeValue.timeValueMinutes(8))
                .setQuery(QueryBuilders.termQuery("type","1"))
                .get();
        Set<String> ids = new HashSet<String>();
        SearchHits searchHits = response.getHits();
        for(SearchHit searchHit : searchHits){
            if(searchHit.getSource().get("title").equals("") &&
                    searchHit.getSource().get("body").equals("")){
                ids.add(searchHit.getId());
            }
        }
        System.out.println(searchHits.getTotalHits());

        String scrollId = response.getScrollId();
        int size = searchHits.getHits().length;
        while(size != 0){
            response = elasticUtil.client.prepareSearchScroll(scrollId)
                    .setScroll(TimeValue.timeValueMinutes(8)).get();
            searchHits = response.getHits();
            for(SearchHit searchHit : searchHits){
                if(searchHit.getSource().get("title").equals("") &&
                        searchHit.getSource().get("body").equals("")){
                    ids.add(searchHit.getId());
                }
            }
            scrollId = response.getScrollId();
            size = searchHits.getHits().length;
        }
        for(String s : ids){
            System.out.println(s);
            elasticUtil.deleteById(s);
        }
    }
}
