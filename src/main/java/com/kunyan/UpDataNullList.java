package com.kunyan;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class UpDataNullList {


    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        try {
            Set<SearchHit> hits = elasticUtil.timeSearchData(args[1],args[2]);
            for(SearchHit searchHit :hits){
                String id = searchHit.getId();
                JSONObject job = new JSONObject();
                ArrayList<String> industries = (ArrayList<String>)searchHit.getSource().get("industries");
                if(industries != null && industries.get(0).equals("")){
                    job.put("industries",null);
                }
                ArrayList<String> sections = (ArrayList<String>)searchHit.getSource().get("sections");
                if(sections != null && sections.get(0).equals("")){
                    job.put("sections",null);
                }
                ArrayList<String> stocks = (ArrayList<String>)searchHit.getSource().get("stocks");
                if(stocks != null && stocks.get(0).equals("")){
                    job.put("stocks",null);
                }
                ArrayList<String> remarks = (ArrayList<String>)searchHit.getSource().get("remarks");
                if(remarks != null && remarks.get(0).equals("")){
                    job.put("remarks",null);
                }
                ArrayList<String> showcase = (ArrayList<String>)searchHit.getSource().get("showcase");
                if(showcase != null && showcase.get(0).equals("")){
                    job.put("showcase",null);
                }
                elasticUtil.updateData(id,JSONObject.toJSONString(job, SerializerFeature.WriteMapNullValue));
            }
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
