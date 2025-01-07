package com.atguigu.edu.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * IK分词器
 */
public class IKUtil {
    public static void main(String[] args) {

        List<String> words = splitWord("小米手机全网通");
        System.out.println(words);
    }

    public static List<String> splitWord(String Word){

        ArrayList<String> words = new ArrayList<>();

        StringReader stringReader = new StringReader(Word);

        IKSegmenter ikSegmenter = new IKSegmenter(stringReader , true);

        Lexeme lexeme =  null ;
        try {
            while( (lexeme = ikSegmenter.next()) != null ){
                //调用 ikSegmenter 的 next 方法获取下一个词元（Lexeme）
                //只要获取到的词元不为 null，就说明还有分词结果可以处理。

                // 取出:获取到的词元 lexeme 中提取出文本内容，也就是实际的分词后的单词。
                String word = lexeme.getLexemeText();

                // 添加到集合中
                words.add( word );
            }
            return words ;
        }catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("分词失败..........." ) ;
        }

    }

 }
