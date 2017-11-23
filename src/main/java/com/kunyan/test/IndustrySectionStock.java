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

        String content = "指数小跌而个股普跌的原因是它\n声明：玉名系头条号签约作者，本文归玉名所有，如转载，请注明文章出处和相关链接。股民想要参与解套课堂和金股课堂，可加玉名微博。　　近期市场走强的还是以蓝筹股为主，贵州茅台、中国平安等有所回落，结果同为上证50的中国神华等马上就启动跟进，说明主力资金对指数控盘度很高。而在大盘连续上涨6个交易日后，市场聚集了大量的获利筹码，有落袋而安的市场需求；股指在连续上涨后，成交量能未能有效放大，所以才有了连续的调整，3400点也一度失守。玉名认为如今股民感觉尴尬的就是个股之前并未跟随指数新高而走强，如今调整却是普跌，该如何处理？实际上，股民应该看懂，指数新高靠的是权重股的拉升，而非大部分个股的重心上移，随着连阳之后的派发压力，以及缺口的吸引，才有了这波调整；而调整之际，资金恐慌撤离，才有了普跌；不过也有明显的热点逆势活跃，这是值得股民重视的。　　今年来白马股集中大涨？一个很关键的因素就是震荡市带来的指数振幅收窄，资金持股集中，8%的个股不断走强，与之带来的就是其余90%以上个股无法上涨，其中大部分则出现了明显的下跌。同时，市值越大的个股表现越好，而行业龙头股更是成为资金集中介入的主要对象，这值得重视。今儿微博订阅文章《谁偷走你的盈利？8%的股占了50%的资金量》做了分析，指数反弹，股民个股没有涨，而市场调整时，却是普跌的。这意味着大多数人没在指数新高中赚钱，却在指数调整时赔钱了。仔细研究可发现8%的个股占据50%市场资金，形成了市场如今独特的选股模式。今年以来涨幅居前股票的机构配置数量都是逐季增加，而跌幅较大股票的机构持股数量在下降。其中最典型就是行业龙头股被资金集中持股。其余热点更多地是局部热点了，次新股、周期股等都是涨跌交替的反复走势，而类似ST摘帽股、超跌股等也是能够在局部活跃，还有是一些行业的崛起机会。今晚20:30分，玉名微博还将对此详解。　　11.14日指数回补了缺口，而主要杀跌个股就是昨天涨幅靠前的中小银行和芯片、5G等题材股，这些个股走弱说明市场热点没有延续性，追涨被套的概率非常大；特别是个股连续上涨2-3天该撤就得撤离，逢低建议考虑连续回调没有上涨的个股，弱势行情整体还是以控制风险为主。玉名认为股民应该注意到每次调整中热点切换的情况，如这两天，避险的黄金股明显走强，资金谨慎情绪浓厚，还有农业股等逆势反弹的情况，最关键的是本周初文章《遵循两思路，挖掘ST摘帽潜力股（附股）》中挖掘的ST摘帽股走出了独立走势，持续走高，值得股民重视。\n“玉名投资家园”公众号：yuming618，每天早盘前准时更新独家文章和金股，欢迎来交流";
        String title = "指数小跌而个股普跌的原因是它";
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
