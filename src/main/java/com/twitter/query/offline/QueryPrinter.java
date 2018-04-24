package com.twitter.query.offline;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Values;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.Set;

public class QueryPrinter
{
    public void analyze(Statement statement)
    {
        new Visitor().process(statement, 1);
    }

    private static final class Visitor
            extends DefaultTraversalVisitor<Void, Integer>
    {
        @Override
        protected Void visitNode(Node node, Integer indentLevel)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitQuery(Query node, Integer indentLevel)
        {
            print(indentLevel, "Query ");

            indentLevel++;

            print(indentLevel, "QueryBody");
            process(node.getQueryBody(), indentLevel);
            if (node.getOrderBy().isPresent()) {
                print(indentLevel, "OrderBy");
                process(node.getOrderBy().get(), indentLevel + 1);
            }

            if (node.getLimit().isPresent()) {
                print(indentLevel, "Limit: " + node.getLimit().get());
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indentLevel)
        {
            print(indentLevel, "QuerySpecification ");

            indentLevel++;

            process(node.getSelect(), indentLevel);

            if (node.getFrom().isPresent()) {
                print(indentLevel, "From");
                process(node.getFrom().get(), indentLevel + 1);
            }

            if (node.getWhere().isPresent()) {
                print(indentLevel, "Where");
                process(node.getWhere().get(), indentLevel + 1);
            }

            if (node.getGroupBy().isPresent()) {
                String distinct = "";
                if (node.getGroupBy().get().isDistinct()) {
                    distinct = "[DISTINCT]";
                }
                print(indentLevel, "GroupBy" + distinct);
                for (GroupingElement groupingElement : node.getGroupBy().get().getGroupingElements()) {
                    print(indentLevel, "SimpleGroupBy");
                    if (groupingElement instanceof SimpleGroupBy) {
                        for (Expression column : ((SimpleGroupBy) groupingElement).getColumnExpressions()) {
                            process(column, indentLevel + 1);
                        }
                    }
                    else if (groupingElement instanceof GroupingSets) {
                        print(indentLevel + 1, "GroupingSets");
                        for (Set<Expression> column : groupingElement.enumerateGroupingSets()) {
                            print(indentLevel + 2, "GroupingSet[");
                            for (Expression expression : column) {
                                process(expression, indentLevel + 3);
                            }
                            print(indentLevel + 2, "]");
                        }
                    }
                    else if (groupingElement instanceof Cube) {
                        print(indentLevel + 1, "Cube");
                        for (QualifiedName column : ((Cube) groupingElement).getColumns()) {
                            print(indentLevel + 1, column.toString());
                        }
                    }
                    else if (groupingElement instanceof Rollup) {
                        print(indentLevel + 1, "Rollup");
                        for (QualifiedName column : ((Rollup) groupingElement).getColumns()) {
                            print(indentLevel + 1, column.toString());
                        }
                    }
                }
            }

            if (node.getHaving().isPresent()) {
                print(indentLevel, "Having");
                process(node.getHaving().get(), indentLevel + 1);
            }

            if (node.getOrderBy().isPresent()) {
                print(indentLevel, "OrderBy");
                process(node.getOrderBy().get(), indentLevel + 1);
            }

            if (node.getLimit().isPresent()) {
                print(indentLevel, "Limit: " + node.getLimit().get());
            }

            return null;
        }

        protected Void visitOrderBy(OrderBy node, Integer indentLevel)
        {
            for (SortItem sortItem : node.getSortItems()) {
                process(sortItem, indentLevel);
            }

            return null;
        }

        protected Void visitJoin(Join node, Integer indentLevel)
        {
            print(indentLevel, "Join");
            ++indentLevel;

            process(node.getLeft(), indentLevel);
            process(node.getRight(), indentLevel);

            final Integer indent = indentLevel;

            node.getCriteria()
                    .filter(criteria -> criteria instanceof JoinOn)
                    .map(criteria -> process(((JoinOn) criteria).getExpression(), indent));

            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indentLevel)
        {
            String distinct = "";
            if (node.isDistinct()) {
                distinct = "[DISTINCT]";
            }
            print(indentLevel, "Select" + distinct);

            super.visitSelect(node, indentLevel + 1); // visit children

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer indent)
        {
            if (node.getPrefix().isPresent()) {
                print(indent, node.getPrefix() + ".*");
            }
            else {
                print(indent, "*");
            }

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent)
        {
            if (node.getAlias().isPresent()) {
                print(indent, "Alias: " + node.getAlias().get());
            }

            super.visitSingleColumn(node, indent + 1); // visit children

            return null;
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, Integer indentLevel)
        {
            print(indentLevel, node.getType().toString());

            super.visitComparisonExpression(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, Integer indentLevel)
        {
            print(indentLevel, node.getType().toString());

            super.visitArithmeticBinary(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, Integer indentLevel)
        {
            print(indentLevel, node.getType().toString());

            super.visitLogicalBinaryExpression(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitStringLiteral(StringLiteral node, Integer indentLevel)
        {
            print(indentLevel, "String[" + node.getValue() + "]");
            return null;
        }

        @Override
        protected Void visitBinaryLiteral(BinaryLiteral node, Integer indentLevel)
        {
            print(indentLevel, "Binary[" + node.toHexString() + "]");
            return null;
        }

        @Override
        protected Void visitBooleanLiteral(BooleanLiteral node, Integer indentLevel)
        {
            print(indentLevel, "Boolean[" + node.getValue() + "]");
            return null;
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, Integer indentLevel)
        {
            print(indentLevel, "Long[" + node.getValue() + "]");
            return null;
        }

        @Override
        protected Void visitLikePredicate(LikePredicate node, Integer indentLevel)
        {
            print(indentLevel, "LIKE");

            super.visitLikePredicate(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, Integer indentLevel)
        {
            print(indentLevel, "Identifier[" + node.getValue() + "]");
            return null;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Integer indentLevel)
        {
            print(indentLevel, "DereferenceExpression[" + node + "]");
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, Integer indentLevel)
        {
            String name = Joiner.on('.').join(node.getName().getParts());
            print(indentLevel, "FunctionCall[" + name + "]");

            super.visitFunctionCall(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indentLevel)
        {
            String name = Joiner.on('.').join(node.getName().getParts());
            print(indentLevel, "Table[" + name + "]");

            return null;
        }

        @Override
        protected Void visitValues(Values node, Integer indentLevel)
        {
            print(indentLevel, "Values");

            super.visitValues(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitRow(Row node, Integer indentLevel)
        {
            print(indentLevel, "Row");

            super.visitRow(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indentLevel)
        {
            print(indentLevel, "Alias[" + node.getAlias() + "]");

            super.visitAliasedRelation(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indentLevel)
        {
            print(indentLevel, "TABLESAMPLE[" + node.getType() + " (" + node.getSamplePercentage() + ")]");

            super.visitSampledRelation(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indentLevel)
        {
            print(indentLevel, "SubQuery");

            super.visitTableSubquery(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitInPredicate(InPredicate node, Integer indentLevel)
        {
            print(indentLevel, "IN");

            super.visitInPredicate(node, indentLevel + 1);

            return null;
        }

        @Override
        protected Void visitSubqueryExpression(SubqueryExpression node, Integer indentLevel)
        {
            print(indentLevel, "SubQuery");

            super.visitSubqueryExpression(node, indentLevel + 1);

            return null;
        }

        private void print(Integer indentLevel, String value)
        {
            System.out.println(Strings.repeat("   ", indentLevel) + value);
        }
    }

    ;
}
