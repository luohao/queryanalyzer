package com.twitter.query.offline;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class ExpressionExtractor
{
    private final QueryAnalysis analysis;

    public ExpressionExtractor(QueryAnalysis analysis)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
    }

    public void analyze(Node node)
    {
        ExpressionExtractor.Visitor extractor = new ExpressionExtractor.Visitor();
        extractor.process(node, null);
        return;
    }

    // Visitor that SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions to pass down to analyzeFrom
    // There are two cases where we need to passdown the candidate names to next level nodes
    //   - Query -> QuerySpecification
    //   - QuerySpecification -> From
    // The candidate names will be registered with Analysis object and use the analysis as a side channel to inform the next pass
    private class Visitor
            extends DefaultTraversalVisitor<List<Expression>, List<Expression>>
    {
        public List<Expression> process(Node node, List<Expression> src)
        {
            return super.process(node, src);
        }

        @Override
        protected List<Expression> visitNode(Node node, List<Expression> src)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected List<Expression> visitQuery(Query node, List<Expression> src)
        {
            ImmutableList.Builder<Expression> candidateBuilder = ImmutableList.builder();
            if (node.getOrderBy().isPresent()) {
                candidateBuilder.addAll(process(node.getOrderBy().get(), emptyList()));
            }

            // pass down the order by candidate filed names to query body
            return process(node.getQueryBody(), candidateBuilder.build());
        }

        @Override
        protected List<Expression> visitQuerySpecification(QuerySpecification node, List<Expression> src)
        {
            // SELECT, WHERE, HAVING, GROUP BY and ORDER BY expressions to pass down to analyzeFrom
            ImmutableList.Builder<Expression> candidateBuilder = ImmutableList.builder();
            // ORDER BY passed down from parent Query object
            candidateBuilder.addAll(src);
            // SELECT
            List<Expression> outputExpressions = process(node.getSelect(), emptyList());
            candidateBuilder.addAll(outputExpressions);

            // GROUP BY
            if (node.getGroupBy().isPresent()) {
                candidateBuilder.addAll(process(node.getGroupBy().get(), emptyList()));
            }
            // WHERE
            if (node.getWhere().isPresent()) {
                candidateBuilder.add(node.getWhere().get());
            }
            // HAVING
            if (node.getHaving().isPresent()) {
                candidateBuilder.add(node.getHaving().get());
            }
            // ORDER BY
            if (node.getOrderBy().isPresent()) {
                candidateBuilder.addAll(process(node.getOrderBy().get(), emptyList()));
            }
            // Pass down expressions to FROM node
            // The from_item is either
            //   - table_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
            //   - from_item join_type from_item [ ON join_condition | USING ( join_column [, ...] ) ]
            // We only care about table_name type since this is the place where we check the metastore
            // and get all columns from the table.
            if (node.getFrom().isPresent()) {
                if (node.getFrom().get() instanceof Table) {
                    Table table = (Table) node.getFrom().get();
                    analysis.registerCandidateFields(table, candidateBuilder.build());
                }
            }

            return outputExpressions;
        }

        @Override
        protected List<Expression> visitSelect(Select node, List<Expression> src)
        {
            ImmutableList.Builder<Expression> candidateBuilder = ImmutableList.builder();
            for (SelectItem item : node.getSelectItems()) {
                if (item instanceof AllColumns) {
                    // XXX: if it selects all columns, there is really nothing we can do.
                    // For queries like
                    //   SELECT * from a group by 1
                    // we will have a problem since there is no reference to the column names,
                    // and the names have to be retrieved from table metadata.
                }
                else if (item instanceof SingleColumn) {
                    // if it selects a single column
                    SingleColumn column = (SingleColumn) item;
                    candidateBuilder.add(column.getExpression());
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }
            return candidateBuilder.build();
        }

        @Override
        protected List<Expression> visitGroupBy(GroupBy node, List<Expression> src)
        {
            ImmutableList.Builder<Expression> candidateBuilder = ImmutableList.builder();
            for (GroupingElement groupingElement : node.getGroupingElements()) {
                candidateBuilder.addAll(process(groupingElement, src));
            }
            return candidateBuilder.build();
        }

        @Override
        protected List<Expression> visitGroupingElement(GroupingElement node, List<Expression> src)
        {
            ImmutableList.Builder<Expression> candidateBuilder = ImmutableList.builder();

            for (Set<Expression> expressions : node.enumerateGroupingSets()) {
                for (Expression groupingColumn : expressions) {
                    if (groupingColumn instanceof LongLiteral) {
                        // if this is a reference to a filed in the output then no need
                        // to extract it because it must present in the output scope
                    }
                    else {
                        candidateBuilder.add(groupingColumn);
                    }
                }
            }
            return candidateBuilder.build();
        }

        @Override
        protected List<Expression> visitOrderBy(OrderBy node, List<Expression> src)
        {
            ImmutableList.Builder<Expression> candidateBuilder = ImmutableList.builder();
            for (SortItem sortItem : node.getSortItems()) {
                // TODO: extract names from each of them
                Expression expression = sortItem.getSortKey();
                if (expression instanceof LongLiteral) {
                    // if this is a reference to a filed in the output then no need
                    // to extract it because it must present in the output scope
                }
                else {
                    // otherwise, add it to candidates
                    candidateBuilder.add(expression);
                }
            }
            return candidateBuilder.build();
        }
    }
}
