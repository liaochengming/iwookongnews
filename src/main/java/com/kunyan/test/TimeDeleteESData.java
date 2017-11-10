package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TimeDeleteESData {

    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile("src/main/resource/config.xml");
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        elasticUtil.searchDelete("2017-11-03","2017-11-04");

    }
}
