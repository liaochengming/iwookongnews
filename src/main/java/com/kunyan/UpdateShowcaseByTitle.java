package com.kunyan;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;

import java.text.ParseException;

public class UpdateShowcaseByTitle {

    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        try {
           SearchHit searchHit =  elasticUtil.selectDataByTitle(args[1]);
           if(searchHit != null ){
               System.out.println("更新的数据标题是：" + searchHit.getSource().get("title"));
//               String id = searchHit.getId();
//               JSONObject job = new JSONObject();
//               job.put("showcase","");
//               elasticUtil.updateData(id,job.toString());
           }else{
               System.out.println("没有此条标题的数据");
           }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
