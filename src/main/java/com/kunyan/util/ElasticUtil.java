package com.kunyan.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kunyan.Scheduler;
import com.kunyan.entity.News;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;


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

    public void updateData(String id, String json) {

        UpdateRequest uRequest = new UpdateRequest();
        uRequest.index(index);
        uRequest.type(type);
        uRequest.id(id);
        try {
            uRequest.doc(json, XContentType.JSON);
            client.update(uRequest).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String selectId(String url) {

        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("url", url))
                .execute().actionGet();
        return response.getHits().getAt(0).getId();
    }

    //查找Es是否包含相似字段
    public String hasFieldLike(String field, String fieldStr, String percent) throws ParseException {

        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setPostFilter(QueryBuilders.rangeQuery("time_spider")//时间过滤
                        .gte(getTime(System.currentTimeMillis() - 3 * 30 * 24 * 60 * 60 * 1000L))
                        .lte(getTime(System.currentTimeMillis())))
                .setQuery(QueryBuilders.matchQuery(field, fieldStr).minimumShouldMatch(percent))//相似度查询
                .execute().actionGet();
        SearchHits searchHits = response.getHits();
        String likeTitle = "";
        for(SearchHit searchHit : searchHits){
            String t = (String)searchHit.getSource().get(field);
            if(likeTitle.equals("")){
                likeTitle = t;
            }else{
                if(likeTitle.length() > t.length()){
                    likeTitle = t;
                }
            }
        }
        return  likeTitle;
    }

    //根据ID删除文档
    public void deleteById(String id){
        DeleteResponse response = client.prepareDelete(index,type,id).get();
    }

    //查询时间范围内的所有文档ID
    public Set<String> searchIds(String timeStart,String timeEnd) throws ExecutionException, InterruptedException {
        Set<String> ids = new HashSet<String>();
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(1000)
                .setScroll(TimeValue.timeValueMinutes(8))
                .setPostFilter(QueryBuilders.rangeQuery("time_spider")//时间过滤
                        .gte(timeStart + " 00:00:00")
                        .lte(timeEnd + " 00:00:00"))
                .get();
        //第一个集合
        SearchHits searchHits = response.getHits();
        for(SearchHit searchHit : searchHits){
            ids.add(searchHit.getId());
        }
        System.out.println(searchHits.getTotalHits());

        String scrollId = response.getScrollId();
        int size = searchHits.getHits().length;
        while(size != 0){
            response = client.prepareSearchScroll(scrollId)
                    .setScroll(TimeValue.timeValueMinutes(8)).get();
            searchHits = response.getHits();
            for(SearchHit searchHit : searchHits){
                ids.add(searchHit.getId());
            }
            scrollId = response.getScrollId();
            size = searchHits.getHits().length;
        }
        return ids;
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
        String cluserName = doc.selectSingleNode("xml/es/cluster").getText();
        try {

            client = new PreBuiltXPackTransportClient(Settings.builder()
                    .put("cluster.name", cluserName)
                    .put("xpack.security.user", userName + ":" + passWord)
                    .put("xpack.security.transport.ssl.enabled", "false")
                    .build()
            );
            for (String esIp : serverIp.split(",")) {
                client.addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(esIp),
                                serverPort));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public String getTime(long time) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(time);
        return sdf.format(date);

    }

    //查找Es是否包含相似字段
    public SearchHits selectWord(String field, String fieldStr, String percent) throws ParseException {
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setPostFilter(QueryBuilders.rangeQuery("time_spider")//时间过滤
//                        .gte(getTime(System.currentTimeMillis() - 3 * 30 * 24 * 60 * 60 * 1000L))
//                        .lte(getTime(System.currentTimeMillis())))
                .setQuery(QueryBuilders.matchQuery(field, fieldStr).minimumShouldMatch(percent))//相似度查询
                .execute().actionGet();
        return response.getHits();
    }


    /**
     *
     * @param timeStart yyyy-MM-dd hh:mm:ss
     * @param timeEnd yyyy-MM-dd hh:mm:ss
     * @return SearchHits
     */
    public Set<SearchHit> timeSearchData(String timeStart,String timeEnd) throws ExecutionException, InterruptedException {
        Set<SearchHit> hitSet = new HashSet<SearchHit>();
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(1000)
                .setScroll(TimeValue.timeValueMinutes(8))
                .setPostFilter(QueryBuilders.rangeQuery("time_spider")//时间过滤
                        .gte(timeStart)
                        .lte(timeEnd))
                .get();
        //第一个集合
        SearchHits searchHits = response.getHits();
        for(SearchHit searchHit : searchHits){
            hitSet.add(searchHit);
        }
        System.out.println(searchHits.getTotalHits());

        String scrollId = response.getScrollId();
        int size = searchHits.getHits().length;
        while(size != 0){
            response = client.prepareSearchScroll(scrollId)
                    .setScroll(TimeValue.timeValueMinutes(8)).get();
            searchHits = response.getHits();
            for(SearchHit searchHit : searchHits){
                hitSet.add(searchHit);
            }
            scrollId = response.getScrollId();
            size = searchHits.getHits().length;
        }
        return hitSet;
    }
}
