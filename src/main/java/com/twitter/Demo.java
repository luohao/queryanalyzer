package com.twitter;

import com.twitter.query.QueryAnalysis;
import com.twitter.query.oline.OnlineQueryAnalyzer;

/**
 * Hello world!
 */
public class Demo
{
    public static void main(String[] args)
    {
        System.out.println("Hello, world!");
        OnlineQueryAnalyzer analyzer = new OnlineQueryAnalyzer();
        QueryAnalysis analysis = analyzer.analyze("select * from a");
        analysis.report();
    }
}
