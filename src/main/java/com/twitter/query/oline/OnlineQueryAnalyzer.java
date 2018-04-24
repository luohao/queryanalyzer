package com.twitter.query.oline;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.twitter.query.QueryAnalysis;

public class OnlineQueryAnalyzer
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    public OnlineQueryAnalyzer() {}

    public QueryAnalysis analyze(String query) {
        Statement statement = SQL_PARSER.createStatement(query);
        QueryAnalysis analysis = new QueryAnalysis(statement);
        return analysis;
    }
}
