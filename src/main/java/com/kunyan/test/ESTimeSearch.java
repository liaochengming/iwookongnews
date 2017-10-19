package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ESTimeSearch {


    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile("src/main/resource/config.xml");
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        try {
            Set<SearchHit> searchHits = elasticUtil.timeSearchData("2017-10-18 17:50:00","2017-10-18 18:00:00");
            for(SearchHit hit : searchHits){
                ArrayList<String> remark;
                remark = (ArrayList<String>)hit.getSource().get("remarks");
                if(remark.size() != 0 && !remark.get(0).equals("")){
                    System.out.println(hit.getSource().get("title"));
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
