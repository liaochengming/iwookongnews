package com.kunyan.test;

import com.alibaba.fastjson.JSONObject;
import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;

import java.util.Set;

public class UpdateSoHu {

    public static void main(String[] args) {


        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        Set<SearchHit> searchHitSet = elasticUtil.getSoHuData();

        for (SearchHit searchHit : searchHitSet) {
            String site = (String)searchHit.getSource().get("site");
            System.out.println(site);
            String url = (String)searchHit.getSource().get("url");
            System.out.println(url);

            String id = searchHit.getId();
            JSONObject job = new JSONObject();
            job.put("site","网易财经");
            elasticUtil.updateData(id, job.toString());
        }
    }
}
