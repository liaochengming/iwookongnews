package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class ESPrefixQuery {

    public static void main(String[] args) {
//        XMLDocument doc = new XMLDocument();
//        doc.loadFile("src/main/resource/config.xml");
//        ElasticUtil elasticUtil = new ElasticUtil(doc);
//        SearchHits searchHits = elasticUtil.searchSomeLike("title","独立意见","80%");
//        for(SearchHit searchHit : searchHits){
//            System.out.println(searchHit.getSource().get("title"));
//        }
        String s = "独立董事关于第四届董事会第次会议相关事项的独立意见";
        String ss = "独立董事关于第四届董事会第七次会议相关事项的独立意见";
        System.out.println(ss.startsWith(s));
    }
}
