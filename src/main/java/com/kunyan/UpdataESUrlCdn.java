package com.kunyan;

import com.alibaba.fastjson.JSONObject;
import com.kunyan.util.ElasticUtil;
import com.nlp.GetInfo;
import com.nlp.util.EasyParser;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import scala.Tuple4;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class UpdataESUrlCdn {

    public static void main(String[] args) {

        Connection conn ;
        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        String mysqlImageUrl = doc.selectSingleNode("xml/mysql_image/url").getText();
        String mysqlImageUser = doc.selectSingleNode("xml/mysql_image/user").getText();
        String mysqlImagePassword = doc.selectSingleNode("xml/mysql_image/password").getText();
        String parseUrl = doc.selectSingleNode("xml/mysql/parseUrl").getText();
        String index = doc.selectSingleNode("xml/es/index").getText();
        String esType = doc.selectSingleNode("xml/es/type").getText();
        String dictPath = doc.selectSingleNode("xml/path/custom_dict").getText();
        EasyParser.apply(parseUrl,dictPath);
        conn = getMysqlConn(mysqlImageUrl,mysqlImageUser,mysqlImagePassword);

        try {
            timeSearchData(args[1],args[2],elasticUtil.client,index,esType,conn,elasticUtil);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
    public static void disposeData(SearchHit searchHit,Connection conn,ElasticUtil elasticUtil){

        List<String> titleLabel;
        List<String> newsLabel;
        List<String> showLabel;
        String urlCDN;

        String id = searchHit.getId();
        String type = (String)searchHit.getSource().get("type");
        String newsTitle = (String)searchHit.getSource().get("title");
        String newsBody = (String)searchHit.getSource().get("body");
        System.out.println(newsTitle);
        Tuple4 tuple4 = getLabelAndCDN(conn,newsTitle,newsBody,type);
        if(type.equals("0") || type.equals("1") || type.equals("2")){
            System.out.println("type :" + type);
        }

        titleLabel = (List<String>) tuple4._2();
        newsLabel = (List<String>)tuple4._3();
        showLabel = (List<String>)tuple4._4();
        urlCDN = (String)tuple4._1();
        System.out.println("titleLabel = " + titleLabel);
        System.out.println("newsLabel = " + newsLabel);
        System.out.println("showLabel = " + showLabel);
        if(urlCDN != null && urlCDN.equals("")){
            urlCDN = null;
        }

        JSONObject job = new JSONObject();
        job.put("title_label",titleLabel);
        job.put("news_label",newsLabel);
        job.put("show_label",showLabel);
        job.put("url_cdn",urlCDN);
        List<String> showCase = new ArrayList<String>();
        showCase.add("网");
        showCase.add("程");
        job.put("showcase",showCase);
        elasticUtil.updateData(id,job.toString());

    }

    public static Tuple4<String,List<String>,List<String>,List<String>> getLabelAndCDN(Connection conn,String title, String content,
                                                                                 String newsType) {

        GetInfo getInfo = GetInfo.apply();
        List<String> titleTags;
        List<String> allTags;

        if(newsType.equals("3") || newsType.equals("4")){

            com.nlp.NewsInfo newsInfo = getInfo.getTagsAnou(title, content);
            titleTags = newsInfo.title_tags();
            allTags = newsInfo.all_tags();

        }else{

            com.nlp.NewsInfo newsInfo = getInfo.getNounsAndTags(title, content);
            titleTags = newsInfo.title_tags();
            allTags = newsInfo.all_tags();
        }

        List<String> urlCDNList = new ArrayList<String>();
        String urlDNDStr = "";

        if(titleTags.size() == 0){
            titleTags = null;
        }else {
            for(String titleTag : titleTags){

                ResultSet resultSetState = getMysqlData(conn,
                        "select state from keyword_list where keyword='" + titleTag + "' and state <> 'invalid'");
                try{
                    assert resultSetState != null;
                    resultSetState.next();
                    int row = resultSetState.getRow();
                    if(row == 1){

                        ResultSet resultSet = getMysqlData(conn,
                                "select url_cdn from image_list where keyword='" + titleTag + "' and state='over'");
                        if(resultSet != null){
                            try {
                                while (resultSet.next()){
                                    urlCDNList.add(resultSet.getString("url_cdn"));
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            if(urlCDNList.size() == 0){
                urlDNDStr = null;
            }else{
                urlDNDStr = urlCDNList.get((int) (Math.random() * urlCDNList.size()));
            }
        }
        List<String> showTags = new ArrayList<String>();
        if(allTags.size() > 3){
            showTags.add(allTags.get(0));
            showTags.add(allTags.get(1));
            showTags.add(allTags.get(2));
        }else if(allTags.size() == 0){
            allTags = null;
            showTags = null;
        }else{
            showTags = allTags;
        }

        if(urlDNDStr != null && !urlDNDStr.equals("")){
            urlDNDStr = "https://image.iwookong.com" + urlDNDStr;
        }
        System.out.println("urlCDN = " + urlDNDStr);
        return new Tuple4(urlDNDStr, titleTags, allTags, showTags);
    }

    public static ResultSet getMysqlData(Connection conn, String selectStr) {

        PreparedStatement ppst = null;
        try {
            ppst = conn.prepareStatement(selectStr);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            if(ppst != null){
                return ppst.executeQuery();
            }else {
                return null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Connection getMysqlConn(String url, String userName, String passWord){

        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection(url, userName, passWord);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     *
     * @param timeStart yyyy-MM-dd hh:mm:ss
     * @param timeEnd yyyy-MM-dd hh:mm:ss
     * @return SearchHits
     */
    public static void timeSearchData(String timeStart,String timeEnd,TransportClient client,String index,
                                         String type, Connection conn, ElasticUtil elasticUtil) throws ExecutionException, InterruptedException {
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(1000)
                .setFetchSource(new String[]{"title","body","type"},new String[]{})
                .setScroll(TimeValue.timeValueMinutes(8))
                .setQuery(QueryBuilders.termsQuery("type","3","4"))
                .setPostFilter(QueryBuilders.rangeQuery("time_spider")//时间过滤
                        .gte(timeStart + " 00:00:00")
                        .lte(timeEnd+ " 00:00:00"))
                .setPostFilter(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("url_cdn")))
                .get();
        //第一个集合
        SearchHits searchHits = response.getHits();
        System.out.println("size: " + searchHits.getTotalHits());
        for(SearchHit searchHit : searchHits){
            disposeData(searchHit,conn,elasticUtil);
        }

        String scrollId = response.getScrollId();
        int size = searchHits.getHits().length;
        while(size != 0){
            response = client.prepareSearchScroll(scrollId)
                    .setScroll(TimeValue.timeValueMinutes(8)).get();
            searchHits = response.getHits();
            for(SearchHit searchHit : searchHits){
                disposeData(searchHit,conn,elasticUtil);
            }
            scrollId = response.getScrollId();
            size = searchHits.getHits().length;
        }
    }


}
