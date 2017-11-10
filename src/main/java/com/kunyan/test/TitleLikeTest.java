package com.kunyan.test;

import com.kunyan.util.ElasticUtil;
import de.mwvb.base.xml.XMLDocument;

import java.text.ParseException;

public class TitleLikeTest {


    public static void main(String[] args) {

        XMLDocument doc = new XMLDocument();
        doc.loadFile("src/main/resource/config.xml");
        ElasticUtil elasticUtil = new ElasticUtil(doc);
        try {
            boolean b = esTitleExist("理工环科：关于变更公司部分会计政策的公告"
                    ,elasticUtil,"4",
                    "http://app.finance.china.com.cn/stock/data/view_notice.php?id=16770820&symbol=002322");
            System.out.println(b);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public static boolean esTitleExist(String title, ElasticUtil elasticUtil, String articleType, String url) throws ParseException {

        String likeTitle = elasticUtil.hasFieldLike("title", title, "100%");
        int likeTitleLength = replaceSign(likeTitle).length();
        int titleLength = replaceSign(title).length();
        int differ = likeTitleLength - titleLength;
        if (articleType.equals("4") && differ == 0 || !likeTitle.equals("") && differ <= 5 && differ >= -5) {
            System.out.println("title: " + title + "\t" + "url: " + url);
            System.out.println("likeTitle: " + likeTitle);
            return true;
        }
        return false;
    }
    public static String replaceSign(String string) {
        return string.replaceAll(" ", "")
                .replaceAll("\\t", "")
                .replaceAll("\\r", "")
                .replaceAll("\\n", "");
    }
}
