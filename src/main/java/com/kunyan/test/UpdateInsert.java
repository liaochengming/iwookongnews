package com.kunyan.test;

import com.alibaba.fastjson.JSONObject;
import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class UpdateInsert {

    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        ElasticUtil elasticUtil = new ElasticUtil(doc);

        Set<String> ids = null;
        try {
            ids = elasticUtil.searchIds();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assert ids != null;
        for (String id : ids) {
            JSONObject job = new JSONObject();
            List<String> list = new ArrayList<String>();
            list.add("网");
            list.add("程");
            job.put("showcase", list);
            elasticUtil.updateData(id, job.toString());

        }
    }
}
