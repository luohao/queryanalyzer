package com.twitter.query;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueryAnalysis
{
    private final String query;
    private Map<Set<Table>, Integer> joinedTables = new HashMap<>();
    private List<Set<QualifiedName>> equiClauseExpressions = new LinkedList<>();
    private Set<QualifiedName> columnsAccessedInWhere = new HashSet<>();
    private Set<QualifiedName> columnsAccessedInGroupBy = new HashSet<>();

    public QueryAnalysis(String query)
    {
        this.query = query;
    }

    public void registerJoinedTablePairs(Table left, Table right)
    {
        Set tablePairs = ImmutableSet.of(left, right);
        int count = 0;
        if (joinedTables.containsKey(tablePairs)) {
            count = joinedTables.get(tablePairs);
        }
        joinedTables.put(tablePairs, count + 1);
    }

    public void registerEquiClauseExpression(QualifiedName first, QualifiedName second)
    {
        equiClauseExpressions.add(ImmutableSet.of(first, second));
    }

    public void registerColumnsInWhere(Set<QualifiedName> columns)
    {
        columnsAccessedInWhere.addAll(columns);
    }

    public void registerColumnsInGroupBy(Set<QualifiedName> columns)
    {
        columnsAccessedInGroupBy.addAll(columns);
    }

    public void report()
    {
        System.out.println("========== Query Analysis ==========");
        {
            System.out.println(">>  Query");
            System.out.println("    " + query);
        }

        {
            System.out.println(">>  Joined Tables");
            for (Set<Table> e : joinedTables.keySet()) {
                // find fully resolved name
                List<String> tableNames = new LinkedList<>();
                e.forEach(table -> {tableNames.add(table.getName().toString());});
                System.out.println("    " + Joiner.on(" <--> ").join(tableNames));
            }
        }

        {
            System.out.println(">>  Fields Checked for Equality in Join Operation");
            for (Set<QualifiedName> fields : equiClauseExpressions) {
                List<String> fieldNames = new LinkedList<>();
                fields.forEach(field -> {fieldNames.add(field.toString());});
                System.out.println("    " + Joiner.on(" == ").join(fieldNames));
            }
        }
        {
            System.out.println(">>  Fields Accessed in Where Clause");
            List<String> fieldNames = new LinkedList<>();
            columnsAccessedInWhere.forEach(field -> {fieldNames.add(field.toString());});
            System.out.println("    " + Joiner.on("\n    ").join(fieldNames));
        }

        {
            System.out.println(">>  Fields Accessed in GroupBy Clause");
            List<String> fieldNames = new LinkedList<>();
            columnsAccessedInGroupBy.forEach(field -> {fieldNames.add(field.toString());});
            System.out.println("    " + Joiner.on("\n    ").join(fieldNames));
        }

        System.out.println("====================================");
    }
}
