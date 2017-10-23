package com.kunyan.test;

import com.kunyan.Scheduler;
import com.nlp.util.EasyParser;
import com.nlp.util.SegmentHan;
import de.mwvb.base.xml.XMLDocument;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;

import static com.kunyan.Scheduler.easyParser;

public class IndustrySectionStock {


    public static void main(String[] args) {
        XMLDocument doc = new XMLDocument();
        doc.loadFile("src/main/resource/config.xml");

        String content = "据外媒报道，谷歌旗下人工智能研究部门DeepMind团队公布了最强版AlphaGo，它完全可以从零基础学习，在短短3天内，成为顶级围棋高手。\n据外媒报道，谷歌旗下人工智能研究部门DeepMind团队公布了最强版AlphaGo，它完全可以从零基础学习，在短短3天内，成为顶级围棋高手。这款名为AlphaGoZero的水平已超过之前所有版本的AlphaGo。DeepMind团队将关于AlphaGoZero的相关研究以论文的形式，刊发在了10月18日的《自然》杂志上。\n人类已经进入“智能时代”，目前包括谷歌等国际科技巨头纷纷进入人工智能领域。世界各国纷纷抢滩布局。权威机构预测数据显示，世界各国都将受益于人工智能，实现经济大幅增长。目前至2030年，人工智能将助推全球生产总值增长12%左右，近10万亿美元。据赛迪研究院预计，2018年，全球人工智能市场规模将达到2697.3亿元，增长率达到17%。中国人工智能市场规模有望突破380亿元，复合增长率为26.3%。普华永道的报告指出，由于人工智能将提高生产力和产品价值，并推动消费增长，零售业、金融服务和医疗保健将是最大受益行业。\n相关概念股：\n赛为智能(行情300044,诊股)：智能化解决系统服务商，公司拥有人工智能技术储备。\n华中数控(行情300161,诊股)：公司与阿里云达成战略合作布局智能制造等有望受益。\n科大讯飞(行情002230,诊股)：是A股人工智能龙头，公司在以“从能听会说到能理解会思考”为目标的讯飞超脑项目上，持续加大投入，在感知智能、认知智能等领域均取得显著研究成果。\n全志科技(行情300458,诊股)：致力于为人工智能提供基础计算平台、SoC+完整解决方案等。\n佳都科技(行情600728,诊股)：重点布局人脸识别技术，公司的人脸识别算法具有建模速度快等特点，已将其应用于智能安防。\n来源为金融界股票频道的作品，均为版权作品，未经书面授权禁止任何媒体转载，否则视为侵权！";
        String title = "谷歌阿尔法狗再进化 人工智能概念受关注(受益股)";
        String mysqlStockUrl = "jdbc:mysql://192.168.1.113/news?user=news&password=news&useUnicode=true&characterEncoding=utf8";
        EasyParser easyParser = EasyParser.apply(mysqlStockUrl);
        String[] remarks = "概念".split(",");
        String industry;
        String section;
        String stock;
        for (String r : remarks) {

            if (r.equals("行业新闻来源")) {
                industry = Arrays.toString(easyParser.parseNews(3, title, content));
                System.out.println("industry " + industry);
            } else if (r.contains("板块") || r.contains("概念")) {
                section = Arrays.toString(easyParser.parseNews(2, title, content));
                System.out.println("section " + section);
            } else if (r.equals("个股新闻来源")) {
                for(int i=0; i<30; i++){
                    easyParser = EasyParser.apply(mysqlStockUrl);
                    stock = Arrays.toString(easyParser.parseNews(1, title, content));
                    System.out.println("stock " + stock);
                }
            }
        }

    }

}
