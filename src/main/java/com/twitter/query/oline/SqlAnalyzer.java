
package com.twitter.query.oline;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.twitter.query.QueryAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public class SqlAnalyzer
{
    private Session session;
    private Analysis analysis;
    private QueryAnalysis result;

    private final Map<Integer, NodeRef<Table>> tableMap;

    public SqlAnalyzer(Session session, Analysis analysis, QueryAnalysis result)
    {
        this.session = session;
        this.analysis = analysis;
        this.result = result;
        tableMap = extractTables(analysis.getStatement());
    }

    public void analyze(Node node)
    {
        new Visitor().process(node, null);
    }

    private class Visitor
            extends DefaultTraversalVisitor<Void, Void>
    {
        public Void process(Node node, Void ctx)
        {
            return super.process(node, ctx);
        }

        @Override
        protected Void visitTable(Table node, Void ctx)
        {
            return null;
        }

        @Override
        protected Void visitJoin(Join join, Void ctx)
        {
            // find out the two joined tables
            Relation left = join.getLeft();
            Relation right = join.getRight();
            if (left instanceof Table &&
                    right instanceof Table &&
                    analysis.getNamedQuery((Table) left) == null &&
                    analysis.getNamedQuery((Table) right) == null) {
                // join two tables
                result.registerJoinedTablePairs((Table) left, (Table) right);

                // find out what fields are evaluated for equality
                if (join.getType() != Join.Type.CROSS && join.getType() != Join.Type.IMPLICIT) {
                    Expression criteria = analysis.getJoinCriteria(join);
                    for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
                        if (conjunct instanceof ComparisonExpression) {
                            ComparisonExpressionType comparisonType = ((ComparisonExpression) conjunct).getType();
                            Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
                            Expression secondExpression = ((ComparisonExpression) conjunct).getRight();

                            // TODO: record complex JOIN operations
                            if (comparisonType == ComparisonExpressionType.EQUAL) {
                                Set<QualifiedName> firstDependencies = SymbolsExtractor.extractNames(firstExpression, analysis.getColumnReferences());
                                Set<QualifiedName> secondDependencies = SymbolsExtractor.extractNames(secondExpression, analysis.getColumnReferences());

                                assert (firstDependencies.size() == 1 && secondDependencies.size() == 1);
                                // TODO: report fully resolved name
                                QualifiedName first = firstDependencies.stream().findFirst().get();
                                QualifiedName second = secondDependencies.stream().findFirst().get();

                                result.registerEquiClauseExpression(first, second);
                            }
                        }
                    }
                }
            }
            super.visitJoin(join, ctx);

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Void ctx)
        {
            // analyze From
            final QualifiedObjectName relationName;
            if (node.getFrom().isPresent() && (node.getFrom().get() instanceof Table)) {
                Table table = (Table) node.getFrom().get();
                relationName = createQualifiedObjectName(session, table, table.getName());
            }
            else {
                relationName = new QualifiedObjectName("*", "*", "*");
            }

            // analyze where
            node.getWhere().ifPresent(where ->
                    result.registerColumnsInWhere(analyzeExpression(relationName, where)));

            // analyze GroupBy
            node.getGroupBy().ifPresent(groupBy ->
                    result.registerColumnsInGroupBy(
                            analysis.getGroupingSets(node).stream()
                                    .flatMap(x -> x.stream())
                                    .flatMap(e -> analyzeExpression(relationName, e).stream())
                                    .collect(toImmutableSet())
                    ));

            super.visitQuerySpecification(node, ctx);
            return null;
        }

        // In the analysis object, we have the following info:
        //   - columnReference : Expression -> FieldId map
        //   - tables : Table -> TableHandle
        // the problem here is, the sourceNode cannot be retrieved from FieldId, thus we can't find the corresponding table for the column.
        // Therefore, the way we restore the link is a bit nasty: we find the hashCode for the sourceNode, and find the original node by
        // looking up the hashCode
        private Set<QualifiedName> analyzeExpression(QualifiedObjectName tableName, Expression expression)
        {
            NameResolver resolver = new NameResolver(analysis.getColumnReferences());
            ImmutableSet.Builder builder = ImmutableSet.builder();
            resolver.process(expression, builder);
            return builder.build();
        }
    }

    private static class TableVisitor
            extends DefaultTraversalVisitor<Void, ImmutableMap.Builder<Integer, NodeRef<Table>>>
    {
        @Override
        protected Void visitTable(Table table, ImmutableMap.Builder<Integer, NodeRef<Table>> builder)
        {
            builder.put(identityHashCode(table), NodeRef.of(table));
            return null;
        }
    }

    // to extract qualified name with prefix
    public static Map<Integer, NodeRef<Table>> extractTables(Node root)
    {
        ImmutableMap.Builder<Integer, NodeRef<Table>> builder = ImmutableMap.builder();
        new TableVisitor().process(root, builder);
        return builder.build();
    }

    private class NameResolver
            extends DefaultTraversalVisitor<Void, ImmutableSet.Builder<QualifiedName>>
    {
        private final Set<NodeRef<Expression>> columnReferences;

        private NameResolver(Set<NodeRef<Expression>> columnReferences)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<QualifiedName> builder)
        {
            // resolve name for node
            // find the relation
            QualifiedObjectName relationName;
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                // we can find the table name
                int hashCode = analysis.getColumnReferenceFields().get(NodeRef.<Expression>of(node)).getRelationId().hashCode();
                if (tableMap.containsKey(hashCode)) {
                    Table table = (Table) findTable(hashCode);
                    relationName = createQualifiedObjectName(session, table, table.getName());
                }
                else {
                    relationName = new QualifiedObjectName("*", "*", "*");
                }
            }
            else {
                relationName = new QualifiedObjectName("*", "*", "*");
            }

            builder.add(resolveName(relationName, DereferenceExpression.getQualifiedName(node)));

            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<QualifiedName> builder)
        {
            // TODO: [refactor] remove duplicate code...
            // resolve name for node
            // find the relation
            QualifiedObjectName relationName;
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                // we can find the table name
                int hashCode = analysis.getColumnReferenceFields().get(NodeRef.<Expression>of(node)).getRelationId().hashCode();
                Node relation = findTable(hashCode);
                if (relation instanceof Table) {
                    Table table = (Table) relation;
                    relationName = createQualifiedObjectName(session, table, table.getName());
                }
                else {
                    relationName = new QualifiedObjectName("*", "*", "*");
                }
            }
            else {
                relationName = new QualifiedObjectName("*", "*", "*");
            }

            builder.add(resolveName(relationName, QualifiedName.of(node.getValue())));

            return null;
        }
    }

    // fully resolve fieldName
    private static QualifiedName resolveName(QualifiedObjectName table, QualifiedName filedName)
    {
        List<String> parts = Lists.reverse(filedName.getParts());

        // Fully resolved name contains four parts:
        //   catalogName + schemaName + tableName + columnName
        String columnName = parts.get(0);
        String tableName = (parts.size() > 1) ? parts.get(1) : table.getObjectName();
        String schemaName = (parts.size() > 2) ? parts.get(2) : table.getSchemaName();
        String catalogName = (parts.size() > 3) ? parts.get(3) : table.getCatalogName();

        return QualifiedName.of(catalogName, schemaName, tableName, columnName);
    }

    private Node findTable(int hashCode)
    {
        return tableMap.get(hashCode).getNode();
    }
}

