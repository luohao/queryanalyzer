package com.twitter.query;

import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryAnalysis
{
    @Nullable
    private final Statement root;
    private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();
    private final Map<NodeRef<Node>, List<Expression>> outputExpressions = new LinkedHashMap<>();
    private final Map<NodeRef<Table>, QualifiedObjectName> tables = new LinkedHashMap<>();
    private final Map<Scope, QualifiedObjectName> tableScopes = new LinkedHashMap<>();

    public QueryAnalysis(@Nullable Statement statement)
    {
        this.root = statement;
    }

    public RelationType getOutputDescriptor()
    {
        return getOutputDescriptor(root);
    }

    public RelationType getOutputDescriptor(Node node)
    {
        return getScope(node).getRelationType();
    }

    public Scope getScope(Node node)
    {
        return tryGetScope(node).orElseThrow(() -> new IllegalArgumentException(String.format("Analysis does not contain information for node: %s", node)));
    }

    public Optional<Scope> tryGetScope(Node node)
    {
        NodeRef<Node> key = NodeRef.of(node);
        if (scopes.containsKey(key)) {
            return Optional.of(scopes.get(key));
        }

        return Optional.empty();
    }

    public void setScope(Node node, Scope scope)
    {
        scopes.put(NodeRef.of(node), scope);
    }

    public void registerTable(Table table, QualifiedObjectName name)
    {
        tables.put(NodeRef.of(table), name);
    }

    public QualifiedObjectName getTableName(Table table)
    {
        return tables.get(NodeRef.of(table));
    }

    public void registerTableScope(Scope scope, QualifiedObjectName name)
    {
        tableScopes.put(scope, name);
    }

    public QualifiedObjectName getTableName(Scope scope)
    {
        return tableScopes.get(scope);
    }

    public void setOutputExpressions(Node node, List<Expression> expressions)
    {
        outputExpressions.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
    }

    // print out a report for the analysis
    public void report()
    {
        System.out.println("Scopes");
        scopes.forEach((k, v) -> System.out.println("  " + v));

        System.out.println("Output Expression");
        outputExpressions.forEach((k, v) -> System.out.println("  " + k + " : " + v));

        System.out.println("Table Accessed");
        tables.forEach((k, v) -> System.out.println("  " + v));

        System.out.println("Table Scopes");
        tables.forEach((k, v) -> System.out.println("  " + k + " : " + v));
    }
}
