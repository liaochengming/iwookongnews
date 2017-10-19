package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.text.ParseException;

public class ESTitleSearch {


    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile("src/main/resource/config.xml");
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        try {
            SearchHits searchHits = elasticUtil.selectWord("title","秒杀日本 中国持有美国国债创一年来新高！","100%");
            for(SearchHit searchHit:searchHits){
                System.out.println(searchHit.getSource().get("title"));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
