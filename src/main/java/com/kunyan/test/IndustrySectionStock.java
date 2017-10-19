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

        String content = "　　中国基金报记者 江右\n　　两大巨头阵营上市潮涌。\n　　简单算，均以人民币计，阿里系投资趣店赚60亿元，腾讯系在搜狗、阅文赚合计预估281-464亿元。\n　　马云马化腾投资的众安保险上市不到一个月，马云投资的趣店昨晚在美上市，成为又一家100亿美元市值新公司。好戏还在后头，马化腾投资的搜狗马上就要在美股上市，马化腾的阅文集团也马上要在香港上市，两者合计至少有100亿美元。\n　　更大的IPO之战还将上演，马云和马化腾投资的公司中，拟上市的不下100家公司，这些公司的总市值将轻松超过10000亿人民币，甚至更高！\n　　1、阿里系趣店在美股上市：大涨21.58%市值近百亿美金\n　　昨日，又一家中国金融科技创业公司趣店登陆美股，这家互联网贷款提供商上市融资约9亿美元，为美国今年以来第四大规模的IPO（首次公开发行）。\n　　上市首日涨21.58%，股价报收29.18美元，总市值96.25亿美元。以最新的1美元兑6.6316元人民币计算，638.29亿元人民币。\n　　这一上市盛宴背后，创业“九死一生”的创始人罗敏，身家和声名暴增，个人持股市值达6349.12万股，持有上市后股份的19.2%，持股市值122.86亿元人民币。\n　　能让趣店市值600多亿（人民币），罗敏身价过百亿（人民币），这与他们背后的那个男人，马云以及阿里巴巴有着密不可分的关系。\n　　趣店的资料显示，蚂蚁金服的全资子公司API（Hong Kong）Investment Limited持股3772.07万股，持股占比11.4%。\n　　根据媒体公开报道，2014年4月成立的趣店，在2015年8月份引来了巨头蚂蚁金服的2亿美元的投资，并且为趣店带来了流量和数据。在支付宝首页的更多，点击进去的二级页面，即可看到趣店的来分期。市场多认为，趣店的流量和数据对支付宝有着很强的依赖性。\n　　当然，如今来看，蚂蚁金服这2亿美元的投资，如今也获得了不菲的收益。如今持股市值为11亿美元，已经升值450%，赚了9亿美元，折合人民币59.68亿元人民币。\n　　2、腾讯系搜狗也将赴美上市，阅文集团即将港股上市\n　　阿里系的独角兽刚上了个趣店，腾讯系其实也早已箭在弦上。腾讯系的搜狗和阅文集团也上市在即，分别登陆美股和港股，只差最后敲钟了。\n　　就在几天前，10月13日晚间，主要业务为搜索的搜狗向美国证券交易委员会提交了招股说明书，计划在纽交所挂牌交易，通过首次公开招股募集最高6亿美元资金。\n　　搜狗的招股说明书中暂未透露首次公开招股的发行价格区间，及股票发行数量。根据招股书，搜狗CEO王小川持股5.5%。今年初，彭博曾援引王小川的消息称，预计美国IPO给予公司的估值在40亿-50亿美元之间。\n　　2013年，腾讯向搜狗注资4.48亿美元，并将旗下的腾讯搜搜业务和其他相关资产并入搜狗，交易完成后腾讯随即获得搜狗完全摊薄后36.5%的股份，而且根据协议腾讯持股比例会增加至40%左右。\n　　目前搜狗的第一大股东为腾讯，持有搜狗151，557，875股B级普通股，占总股本的43.7%，搜狐持有3，720，250股A级普通股，以及1.272亿股B级普通股，占总股本的37.8%。由于AB股分层结构，搜狐仍主导投票权，为公司实际控制人。\n　　作为最大股东，腾讯也为搜狗贡献了最多的搜索流量。招股书显示，截止到2017年6月，腾讯为搜狗带来了38.2%的搜索流量。\n　　虽然搜过尚未上市，但已有媒体给搜狗估值和腾讯算账。按搜狗60亿美元估值计算，一旦IPO成功，腾讯持股43.7%，腾讯将持有搜狗股份价值为，26.22亿美元，减去此前投入的4.48亿美元投资，腾讯将净赚27.74亿美元，约143亿人民币。\n　　值得注意的是，腾讯系不仅有搜狗即将在美上市，旗下文学版块阅文集团也是马上就要在港股登陆，目前已经通过上市聆讯。腾讯控股公告称，腾讯公司建议分拆阅文集团并于香港联合交易所有限公司主板独立上市。香港有最新消息称阅文集团拟暂定于11月8日正式挂牌。\n　　阅文集团由腾讯文学与原盛大文学整合而成，作为引领行业的正版数字阅读平台和文学IP培育平台，阅文旗下囊括QQ阅读、起点中文网等业界知名品牌，《鬼吹灯》、《盗墓笔记》、《琅琊榜》、《择天记》即由阅文集团改编作品输出。\n　　现时腾讯通过多间全资附属公司间接控制阅文集团合共65.38%已发行股份，上市后仍为腾讯的附属公司。媒体消息称，阅文集团上市容易预计为6-8亿美元，\n　　综合保荐人对阅文集团的估值，介于340亿至670亿元港币之间。腾讯持有65%，在221亿港币436亿港币之间，折合为人民币为188亿元-371亿元之间。2014年底，市场的公开消息是腾讯以50亿元收购了盛大文学，如果以此为成本的话，腾讯又赚了138-321亿元。\n　　如果简单笼统计算，可以认为腾讯在搜狗和阅文集团赚281亿元-464亿元。\n　　3、阿里腾讯平安三马联手之众安保险也刚在港股上市\n　　阿里系与腾讯系10亿美元以上市值独角兽上市的案例，最近受关注的，还有个就是众安保险。这个2013年10月份，由马云、马化腾和平安集团马明哲三马联手的互联网保险公司，9月28日在港股上市。\n　　截止昨日收盘，众安在线的市值为1181.52亿港币，折合人民币1004亿元。三马的持股比例，阿里、腾讯、平安持股占比分别为13.82%、10.42%、10.42%。\n　　以持股比例来看，阿里系持股市值为163亿元港币，腾讯系持股市值为123亿元港币，折合人民币分别为139亿元和105亿元。\n　　4、阿里系腾讯系企业上市大PK\n　　阿里系、腾讯系的独角兽们最近出现上市大潮。从两大互联网巨头的种种布局来看，未来这种局面或将继续上演，毕竟两大巨头在各大领域还自己经营或是投资了不少独角兽企业。\n　　这里，通过网上整理搜集了两幅阿里与腾讯的帝国版图，图片或有统计不完全或是更新不及时，但大体反映了，两大巨头的布局，其中还有大量独角兽公司尚未上市。\n　　可以看出，趣店（趣分期）、众安保险只是阿里系金融版块中，布局的一个分子，阿里系重磅的蚂蚁金服尚未上市。\n　　腾讯系版图中，阅文集团为泛文娱版块的一个，众安保险业只是金融版块一分子，而投资布局的滴滴出行、大众点评等也尚未上市。\n　";
        String title = "马云马化腾IPO之战打响：趣店已上市 搜狗还远吗？";
        String mysqlStockUrl = "jdbc:mysql://192.168.1.113/news?user=news&password=news&useUnicode=true&characterEncoding=utf8";
        EasyParser easyParser = EasyParser.apply(mysqlStockUrl);
        String[] remarks = "个股新闻来源".split(",");
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
                stock = Arrays.toString(easyParser.parseNews(1, title, content));
                System.out.println("stock " + stock);
            }
        }

    }

}
