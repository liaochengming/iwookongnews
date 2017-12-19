package com.kunyan.test;


import com.kunyan.util.MySqlUtil;
import com.nlp.GetInfo;
import com.nlp.NewsInfo;
import com.nlp.EasyParser;
import de.mwvb.base.xml.XMLDocument;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;


public class NewsRecommendTest {


    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        String parseUrl = doc.selectSingleNode("xml/mysql/parseUrl").getText();
        String dictPath = doc.selectSingleNode("xml/path/custom_dict").getText();
        EasyParser easyParser =  EasyParser.apply(parseUrl,dictPath);
        String title = "优中选优观点包开始运行（附逆势牛股）";
        String content = "目前A股市场较以前出现了重大变化\n1.价值投资牢牢占据上风，至上从下，从郭嘉对到机构到游资再到散户。\n2.海外资金，社保资金等大型资金的配置必然选择优质的股票\n3.跌多了就涨，涨多了就跌的传统思维已经不适用于当前的投资思路。\n4.去散户化进程大大加快，散户挣钱越来越难，将逐步倒逼投资专业化，机构化。\n对此，我们需要积极的进行调整，转换思路，留住以前中适合的，去掉不适合的，探讨接下来的市场如何才能盈利的办法，为此我们团队进行了大刀阔斧的改革，在选股的时候非常重视基本面，重视财务报表，重视每股收益，为此，我最近还复习了大学时期的专业课。\n不多说了，我们将会在接下来的股票分享中坚决贯彻这一思路，从荣盛石化开始到小A再到今天要分享的小电，都是优中选优的股票。可以说是真正的价值投资股票，当然我们会及时根据市场风向进行调整。\n今天再分享一只稳健向上的股票，现在刚刚开始，有加速向上的迹象。\n1. 二线蓝筹，体量大，容得下大资金进场。\n2.政策支持，央企改革典范，这种股票上涨会被监管层接受。\n3‍.最值得推荐的是技术形态，底部横盘一年半，低点缓慢抬高，短期中期长期趋势线稳步向上，上周大跌第二天即阳包阴收回，主力非常强大，现在布局正合适。‍\n见私密 ";
        GetInfo getInfo = GetInfo.apply();
        NewsInfo newsInfo = getInfo.getNounsAndTags(title,content);
        List<String> titleTags = newsInfo.title_tags();
        List<String> allTags = newsInfo.all_tags();
        System.out.println(titleTags);
        System.out.println(allTags);

    }
}
