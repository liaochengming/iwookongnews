package com.kunyan.test;

import com.kunyan.entity.News;
import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class AddNews {
    public static void main(String[] args) {

        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        elasticUtil.createIndex(elasticUtil.CreateNews(new News("0","","123","lcm","www.123",
                "2017-11-17","123456", new ArrayList<String>(),new ArrayList<String>(),
                new ArrayList<String>(),0.1f,0.1f,0.1f,"asd",
                true,new ArrayList<String>(),"asd","23","1922-11-11 00:00:00",
                new ArrayList<String>())));
        elasticUtil.createIndex(elasticUtil.CreateNews(new News("4","","","lcm","www.123",
                "2017-11-17","123456", new ArrayList<String>(),new ArrayList<String>(),
                new ArrayList<String>(),0.1f,0.1f,0.1f,"asd",
                true,new ArrayList<String>(),"asd","23","1922-11-11 00:00:00",
                new ArrayList<String>())));
        elasticUtil.createIndex(elasticUtil.CreateNews(new News("4","","","lcm","www.123",
                "2017-11-17","123456", new ArrayList<String>(),new ArrayList<String>(),
                new ArrayList<String>(),0.1f,0.1f,0.1f,"",
                true,new ArrayList<String>(),"asd","23","1922-11-11 00:00:00",
                new ArrayList<String>())));
    }

}
