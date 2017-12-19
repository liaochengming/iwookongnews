package com.kunyan.test;

import com.nlp.util.SegmentHan;

public class SummaryTest {


    public static void main(String[] args) {
        String title = "";
        String content = "【李小加：我是新经济的“恐龙” 从未用过微信支付】港交所行政总裁李小加说：“我是恐龙式的人物，我有时候经常用现金。”他提到自己有一次去台湾吃一碗牛肉面的经历，用现金付款时被店家要求用微信支付。他笑言“像我们这样的恐龙来管理市场，不知道大家会不会有很沉重的感觉。”（新浪）\n";

        String summary = SegmentHan.getSummary(title, content);
        System.out.println(summary);
    }
}
