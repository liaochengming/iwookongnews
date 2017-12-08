package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;

import java.util.Set;
import java.util.concurrent.ExecutionException;

public class DeleteCDNDataTest {
    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        ElasticUtil elasticUtil = new ElasticUtil(doc);

        try {
            Set<SearchHit> searchHits = elasticUtil.timeSearchData("2017-11-30","2017-12-01");
            for(SearchHit searchHit :searchHits){

                String urlCdn = (String)searchHit.getSource().get("url_cdn");
                if(urlCdn!= null && urlCdn.equals("https://image.iwookong.com")){
                    String id = searchHit.getId();
                    elasticUtil.deleteById(id);
                }
            }


        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
