package com.twitter;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.twitter.query.QueryAnalysis;
import com.twitter.query.QueryAnalyzer;
import com.twitter.query.QueryPrinter;

import java.util.Optional;

/**
 * Hello world!
 */
public class App
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = new ParsingOptions();

    public static void main(String[] args)
    {
        String query;
        //query = "select a from (select c from d) where e = f";
        //query = "select a from b where e = f";
        //query = "WITH foo as (select a from b), bar as (select c from d) SELECT x, count(1) FROM foo JOIN bar ON foo.a = bar.y WHERE z IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, b";
        //query = "SELECT x, count(1) FROM foo JOIN bar ON foo.a = bar.y WHERE z IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, b";
        //query = "select a from b";
        query = "select * from b";

        boolean printQuery = false;
        if (printQuery) {
            Statement statement = SQL_PARSER.createStatement(query, PARSING_OPTIONS);
            QueryPrinter queryPrinter = new QueryPrinter();
            queryPrinter.analyze(statement);
        }
        else {
            Statement statement = SQL_PARSER.createStatement(query, PARSING_OPTIONS);
            QueryAnalysis analysis = new QueryAnalysis(statement);
            QueryAnalyzer analyzer = new QueryAnalyzer(analysis, "hive", "default");
            analyzer.analyze(statement, Optional.empty());

            analysis.report();
        }
    }
}
