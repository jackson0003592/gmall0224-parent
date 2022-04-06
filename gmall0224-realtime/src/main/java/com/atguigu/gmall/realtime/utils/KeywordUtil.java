package com.atguigu.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: Felix
 * Date: 2021/8/14
 * Desc: 使用IK分词器进行分词
 */
public class KeywordUtil {
    //分词方法
    public static List<String> analyze(String text){
        StringReader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader,true);
        List<String> resList = new ArrayList<>();
        try {
            Lexeme lexeme = null;
            while ((lexeme = ikSegmenter.next())!=null){
                resList.add(lexeme.getLexemeText());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resList;
    }

    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));

    }
}
