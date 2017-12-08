package com.kunyan.test;

import de.mwvb.base.xml.XMLDocument;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SendMessageKafkaTest {

    public static void main(String[] args) {

        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        String newNews = doc.selectSingleNode("xml/kafka/newsreceive").getText();
        String brokerList = doc.selectSingleNode("xml/kafka/brokerList").getText();

        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put("bootstrap.servers", brokerList);
        kafkaProducerProps.put("acks", "all");
        kafkaProducerProps.put("retries", 0);
        kafkaProducerProps.put("batch.size", 16384);
        kafkaProducerProps.put("linger.ms", 1);
        kafkaProducerProps.put("buffer.memory", 33554432);
        kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProducerProps);
        for(int i=0; i<1000000 ; i++){
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>(newNews, "","{\"task_id\":\"\",\"platform\":\"6\",\"hbase_table_name\":\"new_news\",\"hbase_rowkey\":\"http:\\/\\/stock.10jqka.com.cn\\/20171208\\/c601956521.shtml\",\"update\":true}");
            producer.send(producerRecord);
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
