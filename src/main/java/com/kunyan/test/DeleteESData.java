package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;

import java.text.ParseException;

public class DeleteESData {


    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        ElasticUtil elasticUtil = new ElasticUtil(doc);

        switch (Integer.valueOf(args[2])){
            case 0 ://标题
                try {
                    SearchHit searchHit = elasticUtil.selectDataByTitle(args[1]);
                    elasticUtil.deleteById(searchHit.getId());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            case 1 ://url
                String id = elasticUtil.selectId(args[1]);
                elasticUtil.deleteById(id);
                break;
        }
    }
}
