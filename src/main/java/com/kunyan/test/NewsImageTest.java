package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import com.nlp.util.EasyParser;
import de.mwvb.base.xml.XMLDocument;
import scala.Tuple4;

import java.sql.Connection;
import java.util.List;

import static com.kunyan.UpdataESUrlCdn.getLabelAndCDN;
import static com.kunyan.UpdataESUrlCdn.getMysqlConn;

public class NewsImageTest {

    private static Connection conn ;
    public static void main(String[] args) {

        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        //ElasticUtil elasticUtil = new ElasticUtil(doc);
        String mysqlImageUrl = doc.selectSingleNode("xml/mysql_image/url").getText();
        String mysqlImageUser = doc.selectSingleNode("xml/mysql_image/user").getText();
        String mysqlImagePassword = doc.selectSingleNode("xml/mysql_image/password").getText();
        String parseUrl = doc.selectSingleNode("xml/mysql/parseUrl").getText();
        EasyParser.apply(parseUrl,"D:\\javaUtil-jar\\CustomDict.txt");
        conn = getMysqlConn(mysqlImageUrl,mysqlImageUser,mysqlImagePassword);
        List<String> titleLabel;
        List<String> newsLabel;
        List<String> showLabel;
        String urlCDN;

        String newsTitle = "历时10年酝酿 巴塞尔协议III终于达成";
        String newsBody = "　　高盛发表研究报告，重申李宁(02331)“买入”评级，12个月目标价为8元。该行称，公司股价在11月17日触及7.11元近期高位后回吐16%，相信是由于市场对其双十一网上销售增长放缓感到忧虑。不过，基于最近与管理层的对话，10月及11月份的线下同店销售取得健康的中单位数增长，而全年的网上销售正符合管理层订下的40至50%增长目标，认为今季至今的销售表现反映出公司复苏表现良好。\n　　高盛表示，市场应将焦点放在公司第四季整体平台的销售增长改善上。李宁已升级其零售店铺，以及在今年至今与批发伙伴合作提升其品牌、零售管理及存货控制。相信随着公司的线下生产力提升，以及线上渗透率增加，将持续支持其成年人产品在2017至2019年有9至11%的增长。";

        Tuple4 tuple4 = getLabelAndCDN(conn,newsTitle,newsBody,"3");
        titleLabel = (List<String>) tuple4._2();
        newsLabel = (List<String>)tuple4._3();
        showLabel = (List<String>)tuple4._4();
        urlCDN = (String)tuple4._1();
        System.out.println("titleLabel = " + titleLabel);
        System.out.println("newsLabel = " + newsLabel);
        System.out.println("showLabel = " + showLabel);
        System.out.println("urlCDN = " + urlCDN);

    }
}
