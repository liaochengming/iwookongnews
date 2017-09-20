package com.kunyan.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by Administrator on 2017/9/1.
 * 新闻实体
 */
public class News {

    private String type;//新闻类型
    private String title;
    private String summary;//摘要
    private String site;//来源站名
    private String url;

    @JsonProperty(value = "news_date")
    private String newsDate;//日期

    @JsonProperty(value = "news_time")
    private String newsTime;//时间
    private List<String> industries;//行业
    private List<String> sections;//板块
    private List<String> stocks;//股票

    @JsonProperty(value = "positive_rate")
    private float positiveRate;

    @JsonProperty(value = "neutral_rate")
    private float neutralRate;

    @JsonProperty(value = "passive_rate")
    private float passiveRate;

    @JsonProperty(value = "body")
    private String content;

    private boolean related;

    private List<String> remark;

    private String tags;

    @JsonProperty(value = "news_id")
    private String newsId;

    public boolean isRelated() {
        return related;
    }

    public String getNewsId() {
        return newsId;
    }

    public void setNewsId(String newsId) {
        this.newsId = newsId;
    }

    public News(String type, String title, String summary,
                String site, String url, String newsDate,
                String newsTime, List<String> industries, List<String> sections,
                List<String> stocks, float positiveRate, float neutralRate,
                float passiveRate, String content,boolean related,List<String> remark,
                String tags,String newsId) {
        this.type = type;
        this.title = title;
        this.summary = summary;
        this.site = site;
        this.url = url;
        this.newsDate = newsDate;
        this.newsTime = newsTime;
        this.industries = industries;
        this.sections = sections;
        this.stocks = stocks;
        this.positiveRate = positiveRate;
        this.neutralRate = neutralRate;
        this.passiveRate = passiveRate;
        this.content = content;
        this.related = related;
        this.remark = remark;
        this.tags = tags;
        this.newsId = newsId;
    }

    public String getType() {
        return type;
    }

    public String getTitle() {
        return title;
    }

    public String getSummary() {
        return summary;
    }

    public String getSite() {
        return site;
    }

    public String getUrl() {
        return url;
    }

    public String getNewsDate() {
        return newsDate;
    }

    public String getNewsTime() {
        return newsTime;
    }

    public List<String> getIndustries() {
        return industries;
    }

    public List<String> getSections() {
        return sections;
    }

    public List<String> getStocks() {
        return stocks;
    }

    public float getPositiveRate() {
        return positiveRate;
    }

    public float getNeutralRate() {
        return neutralRate;
    }

    public float getPassiveRate() {
        return passiveRate;
    }

    public String getContent() {
        return content;
    }


    public void setType(String type) {
        this.type = type;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public boolean getRelated() {
        return related;
    }

    public void setRelated(boolean related) {
        this.related = related;
    }

    public List<String> getRemark() {
        return remark;
    }

    public void setRemark(List<String> remark) {
        this.remark = remark;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setNewsDate(String newsDate) {
        this.newsDate = newsDate;
    }

    public void setNewsTime(String newsTime) {
        this.newsTime = newsTime;
    }

    public void setIndustries(List<String> industries) {
        this.industries = industries;
    }

    public void setSections(List<String> sections) {
        this.sections = sections;
    }

    public void setStocks(List<String> stocks) {
        this.stocks = stocks;
    }

    public void setPositiveRate(float positiveRate) {
        this.positiveRate = positiveRate;
    }

    public void setNeutralRate(float neutralRate) {
        this.neutralRate = neutralRate;
    }

    public void setPassiveRate(float passiveRate) {
        this.passiveRate = passiveRate;
    }

    public void setContent(String content) {
        this.content = content;
    }

}
