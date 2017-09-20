package com.kunyan.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kunyan.entity.News;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;


/**
 * Created by Administrator on 2017/9/1.
 * Elastic工具类
 */
public class ElasticUtil {


    final static Logger logger = LoggerFactory.getLogger(ElasticUtil.class);

    String index;
    String type;
    XMLDocument doc;

    public TransportClient client = null;

    public ElasticUtil(XMLDocument doc) {
        this.doc = doc;
        this.index = doc.selectSingleNode("xml/es/index").getText();
        this.type = doc.selectSingleNode("xml/es/type").getText();
        initConn();
    }


    public boolean createIndex(String json) {

        IndexResponse response = client.prepareIndex(index, type).setSource(json, XContentType.JSON).get();
        if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
            logger.info("error status:" + response.status());
            return false;
        }
        return true;
    }

    public void updateData(String id,String json){

        UpdateRequest uRequest = new UpdateRequest();
        uRequest.index(index);
        uRequest.type(type);
        uRequest.id(id);
        try {
            uRequest.doc(json,XContentType.JSON);
            client.update(uRequest).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String selectId(String url){

        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("url", url))
                .execute().actionGet();
        return response.getHits().getAt(0).getId();
    }

    public String CreateNews(News news) {

        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(news);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return json;
    }

    public boolean initConn() {

        String userName = doc.selectSingleNode("xml/es/user_name").getText();
        String passWord = doc.selectSingleNode("xml/es/pass_word").getText();
        String serverIp = doc.selectSingleNode("xml/es/serverIp").getText();
        int serverPort = Integer.valueOf(doc.selectSingleNode("xml/es/server_port").getText());
        String cluserName = "news";
        try {

            client = new PreBuiltXPackTransportClient(Settings.builder()
                    .put("cluster.name", cluserName)
                    .put("xpack.security.user", userName + ":" + passWord)
                    .put("xpack.security.transport.ssl.enabled", "false")
                    .build()
            ).addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName(serverIp), serverPort)
            );
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
