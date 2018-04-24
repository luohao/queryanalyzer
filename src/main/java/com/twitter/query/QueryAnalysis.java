package com.twitter.query;

import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.tree.Statement;

import java.util.Collections;

public class QueryAnalysis
        extends Analysis
{
    public QueryAnalysis(Statement root)
    {
        super(root, Collections.emptyList(), false);
    }

    public void report() {
        System.out.println("QueryAnalysis");
    }
}
