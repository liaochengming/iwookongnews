package com.kunyan.conf;

/**
 * Created by Administrator on 2017/9/6.
 * 平台信息
 */
public enum Platform {

    THS_STOCk(1,"同花顺股票"),
    ZHI_HU(2, "知乎"),
    YI_CAI(3, "第一财经"),
    WEI_BO(4, "微博"),
    CN(5, "21CN"),
    ThS_NEWS(6, "同花顺新闻"),
    SNOW_BALL(7, "雪球"),
    DA_ZHI_HUI(8, "大智慧"),
    EAST_MONEY(9, "东方财富"),
    GOVERNMENT_VALUE(10, "政府网"),
    QUAN_JING(11, "全景网"),
    HE_XUN(12, "和讯"),
    STOCK_STAR(13, "证券之星"),
    CAI_JING(14, "财经网"),
    JRJ(15, "金融界"),
    CFI(16, "中国财经信息网"),
    ZhONG_ZhENG(17, "中证网"),
    CN_STOCK(18, "上海证券报"),
    STCN(19, "证券时报网·中国"),
    XIN_HUA(20, "新华网财经"),
    FENG_HUANG(21, "凤凰财经"),
    SINA(22, "新浪财经"),
    SOU_HU(23, "搜狐财经"),
    NET_EASE(24, "网易财经"),
    WALL_STREET(25, "华尔街见闻"),
    TENCENT(26,"腾讯财经"),
    ChINA_COM(27,"中国网"),
    INTERNATIONAL_FINANCE(28,"国际金融报"),
    GLOBAL_TIGER_NETWORK(29,"环球老虎网"),
    SUPERIOR_WEALTH(30,"优品财富"),
    SMART_FINANCE_NETWORJ(31,"智通财经网 "),
    CHINA_YANG_NETWORK(32,"中青网"),
    CAI_LIAN_PRESS_NEWS(10001, "财联社新闻"),
    IGOLDENBETA_SELF_MEDIA(30004, "金贝塔"),
    WECHAT_SELF_MEDIA(30006, "微信"),
    SHAGN_HAI_STOCK_EXCHANGE(50001, "上海证券交易所"),
    SHEN_ZHEN_STOCK_EXCHANGE(50002, "深圳证券交易所"),
    CN_INFO(50003,"巨潮资讯"),
    AI_YAN_BAO(60001, "爱研报"),
    HE_XUN_YAN_BAO(60012,"和讯研报"),
    EAST_MONEY_YAN_BAO(60013,"东方财富研报");

    private String name ;
    private int num ;

    private Platform(int num,String name){
        this.num = num;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }


    @Override
    public String toString() {
        return this.name;
    }
}
