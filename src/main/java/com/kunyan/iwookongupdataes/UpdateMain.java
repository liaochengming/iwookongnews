package com.kunyan.iwookongupdataes;

import com.kunyan.thread.UpdateEsThread;
import com.kunyan.util.ElasticUtil;
import com.kunyan.util.MyHbaseUtil;
import com.nlp.EasyParser;
import de.mwvb.base.xml.XMLDocument;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UpdateMain {

    public static void main(String[] args) {

        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);

        ElasticUtil elasticUtil = new ElasticUtil(doc);
        String brokerList = doc.selectSingleNode("xml/kafka/brokerList").getText();
        String groupId = doc.selectSingleNode("xml/kafka/groupId").getText();
        String newNewsUpdate = doc.selectSingleNode("xml/kafka/newsreceive_update").getText();

        String rootDir = doc.selectSingleNode("xml/hbase/rootDir").getText();
        String ip = doc.selectSingleNode("xml/hbase/ip").getText();
        MyHbaseUtil hbaseUtil = new MyHbaseUtil(rootDir, ip);

        String dictPath = doc.selectSingleNode("xml/path/custom_dict").getText();
        String mysqlStockUrl = doc.selectSingleNode("xml/mysql/parseUrl").getText();
        EasyParser easyParser = EasyParser.apply(mysqlStockUrl,dictPath);

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.put("bootstrap.servers", brokerList);
        kafkaConsumerProps.put("group.id", groupId);
        kafkaConsumerProps.put("enable.auto.commit", "true");
        kafkaConsumerProps.put("auto.commit.interval.ms", "1000");
        kafkaConsumerProps.put("session.timeout.ms", "30000");
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaConsumerProps);
        consumer.subscribe(Collections.singletonList(newNewsUpdate));

        ExecutorService executorService = Executors.newFixedThreadPool(Integer.valueOf(args[1]));
        String value;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                value = record.value();
                executorService.execute(new UpdateEsThread(value, elasticUtil, hbaseUtil,easyParser, record.offset()));
            }

        }
    }
}
