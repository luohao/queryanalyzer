package com.twitter.query.offline;

import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SingleColumn;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getLast;
import static com.twitter.query.offline.UnknownType.UNKNOWN;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class QueryAnalyzer
{
    private final String catalogName;
    private final String schemaName;
    private final QueryAnalysis analysis;

    public QueryAnalyzer(
            QueryAnalysis analysis,
            String catalogName,
            String schemaName
    )
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
    }

    public Scope analyze(Node node, Scope outerQueryScope)
    {
        return analyze(node, Optional.of(outerQueryScope));
    }

    public Scope analyze(Node node, Optional<Scope> outerQueryScope)
    {
        return new Visitor(outerQueryScope).process(node, Optional.empty());
    }

    /**
     * Visitor that builds up the scope hierarchy.
     */
    private class Visitor
            extends DefaultTraversalVisitor<Scope, Optional<Scope>>
    {
        private final Optional<Scope> outerQueryScope;

        private Visitor(Optional<Scope> outerQueryScope)
        {
            this.outerQueryScope = outerQueryScope;
        }

        public Scope process(Node node, Optional<Scope> scope)
        {
            Scope returnScope = super.process(node, scope);
            checkState(returnScope.getOuterQueryParent().equals(outerQueryScope), "result scope should have outer query scope equal with parameter outer query scope");
            if (scope.isPresent()) {
                checkState(hasScopeAsLocalParent(returnScope, scope.get()), "return scope should have outerQueryScope scope as one of ancestors");
            }
            return returnScope;
        }

        private Scope process(Node node, Scope scope)
        {
            return process(node, Optional.of(scope));
        }

        // visitors
        @Override
        protected Scope visitNode(Node node, Optional<Scope> outerQueryScope)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Scope visitQuery(Query node, Optional<Scope> outerQueryScope)
        {
            //throw new UnsupportedOperationException("not yet implemented: " + node);
            Scope withScope = analyzeWith(node, outerQueryScope);

            Scope queryBodyScope = process(node.getQueryBody(), withScope);

            if (node.getOrderBy().isPresent()) {
                analyzeOrderBy(node, queryBodyScope);
            }

            analysis.setOutputExpressions(node, descriptorToFields(queryBodyScope));

            // build query scope
            Scope queryScope = Scope.builder()
                    .withParent(withScope)
                    .withRelationType(RelationId.of(node), queryBodyScope.getRelationType())
                    .build();

            analysis.setScope(node, queryScope);

            return queryScope;
        }

        @Override
        protected Scope visitQuerySpecification(QuerySpecification node, Optional<Scope> scope)
        {
            Scope sourceScope = analyzeFrom(node, scope);

            node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));

            List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
            List<List<Expression>> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
            analyzeHaving(node, sourceScope);

            Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);

            List<Expression> orderByExpressions = emptyList();
            Optional<Scope> orderByScope = Optional.empty();
            if (node.getOrderBy().isPresent()) {
                orderByScope = Optional.of(computeAndAssignOrderByScope(node.getOrderBy().get(), sourceScope, outputScope));
                orderByExpressions = analyzeOrderBy(node, orderByScope.get(), outputExpressions);
            }

            List<Expression> sourceExpressions = new ArrayList<>();
            sourceExpressions.addAll(outputExpressions);
            node.getHaving().ifPresent(sourceExpressions::add);

            analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
            List<FunctionCall> aggregations = analyzeAggregations(node, sourceScope, orderByScope, groupByExpressions, sourceExpressions, orderByExpressions);
            analyzeWindowFunctions(node, outputExpressions, orderByExpressions);

            if (!groupByExpressions.isEmpty() && node.getOrderBy().isPresent()) {
                // Create a different scope for ORDER BY expressions when aggregation is present.
                // This is because planner requires scope in order to resolve names against fields.
                // Original ORDER BY scope "sees" FROM query fields. However, during planning
                // and when aggregation is present, ORDER BY expressions should only be resolvable against
                // output scope, group by expressions and aggregation expressions.
                computeAndAssignOrderByScopeWithAggregation(node.getOrderBy().get(), sourceScope, outputScope, aggregations, groupByExpressions, analysis.getGroupingOperations(node));
            }

            return outputScope;
        }

        @Override
        protected Scope visitSetOperation(SetOperation node, Optional<Scope> scope)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        // Helper functions for analyzing components
        private Scope analyzeWith(Query node, Optional<Scope> scope)
        {

            if (!node.getWith().isPresent()) {
                return createScope(scope);
            }
            else {
                throw new UnsupportedOperationException("not yet implemented: " + new Object() {}.getClass().getEnclosingMethod().getName());
            }
        }

        private Scope analyzeFrom(QuerySpecification node, Optional<Scope> scope)
        {
            if (node.getFrom().isPresent()) {
                return process(node.getFrom().get(), scope);
            }

            return createScope(scope);
        }

        private List<Expression> analyzeOrderBy(Query node, Scope orderByScope)
        {
            // TODO: analyze ORDERBY clause
            throw new UnsupportedOperationException("not yet implemented: " + new Object() {}.getClass().getEnclosingMethod().getName());
        }

        private List<Expression> analyzeOrderBy(QuerySpecification node, Scope orderByScope, List<Expression> outputExpressions)
        {
            // TODO: analyze ORDERBY clause
            throw new UnsupportedOperationException("not yet implemented: " + new Object() {}.getClass().getEnclosingMethod().getName());
        }

        private void analyzeWhere(Node node, Scope scope, Expression predicate)
        {
            throw new UnsupportedOperationException("not yet implemented: " + new Object() {}.getClass().getEnclosingMethod().getName());
        }

        private void analyzeHaving(QuerySpecification node, Scope scope)
        {
            throw new UnsupportedOperationException("not yet implemented: " + new Object() {}.getClass().getEnclosingMethod().getName());
        }

        private List<Expression> analyzeSelect(QuerySpecification node, Scope scope)
        {
            throw new UnsupportedOperationException("not yet implemented: " + new Object() {}.getClass().getEnclosingMethod().getName());
        }

        private List<List<Expression>> analyzeGroupBy(QuerySpecification node, Scope sourceScope, List<Expression> outputExpressions)
        {
            // TODO: implement analyzeGroupeBy
            if (node.getGroupBy().isPresent()) {
                throw new UnsupportedOperationException("not yet implemented: " + new Object() {}.getClass().getEnclosingMethod().getName());
            }

            return ImmutableList.of();
        }

        private List<Expression> analyzeHaving(Expression expression, Optional<Scope> scope)
        {
            throw new UnsupportedOperationException("not yet implemented: " + new Object() {}.getClass().getEnclosingMethod().getName());
        }

        private void analyzeGroupingOperations(QuerySpecification node, List<Expression> outputExpressions, List<Expression> orderByExpressions)
        {
            List<GroupingOperation> groupingOperations = extractExpressions(Iterables.concat(outputExpressions, orderByExpressions), GroupingOperation.class);
            boolean isGroupingOperationPresent = !groupingOperations.isEmpty();

            if (isGroupingOperationPresent && !node.getGroupBy().isPresent()) {
                throw new SemanticException(
                        INVALID_PROCEDURE_ARGUMENTS,
                        node,
                        "A GROUPING() operation can only be used with a corresponding GROUPING SET/CUBE/ROLLUP/GROUP BY clause");
            }

            analysis.setGroupingOperations(node, groupingOperations);
        }

        // Scope related utility functions
        private List<Expression> descriptorToFields(Scope scope)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (int fieldIndex = 0; fieldIndex < scope.getRelationType().getAllFieldCount(); fieldIndex++) {
                FieldReference expression = new FieldReference(fieldIndex);
                builder.add(expression);
                //analyzeExpression(expression, scope);
            }
            return builder.build();
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope)
        {
            return createAndAssignScope(node, parentScope, emptyList());
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, Field... fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignTableScope(Node node, QualifiedObjectName name, Optional<Scope> parentScope, List<Field> fields)
        {
            // register the table
            Scope tableScope = createAndAssignScope(node, parentScope, new RelationType(fields));
            analysis.registerTableScope(tableScope, name);
            return tableScope;
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, List<Field> fields)
        {
            return createAndAssignScope(node, parentScope, new RelationType(fields));
        }

        private Scope createAndAssignScope(Node node, Optional<Scope> parentScope, RelationType relationType)
        {
            Scope scope = scopeBuilder(parentScope)
                    .withRelationType(RelationId.of(node), relationType)
                    .build();

            analysis.setScope(node, scope);
            return scope;
        }

        private Scope computeAndAssignOrderByScope(OrderBy node, Scope sourceScope, Scope outputScope)
        {
            // ORDER BY should "see" both output and FROM fields during initial analysis and non-aggregation query planning
            Scope orderByScope = Scope.builder()
                    .withParent(sourceScope)
                    .withRelationType(outputScope.getRelationId(), outputScope.getRelationType())
                    .build();
            analysis.setScope(node, orderByScope);
            return orderByScope;
        }

        // create a scope with all fields in SELECT statement
        private Scope computeAndAssignOutputScope(QuerySpecification node, Optional<Scope> scope, Scope sourceScope)
        {
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

            for (SelectItem item : node.getSelect().getSelectItems()) {
                if (item instanceof AllColumns) {
                    // if we see select *, try to add all fields from the output of FROM node
                    Optional<QualifiedName> starPrefix = ((AllColumns) item).getPrefix();

                    for (Field field : sourceScope.getRelationType().resolveFieldsWithPrefix(starPrefix)) {
                        outputFields.add(Field.newUnqualified(field.getName(), field.getType(), field.getOriginTable(), false));
                    }
                }
                else if (item instanceof SingleColumn) {
                    SingleColumn column = (SingleColumn) item;

                    Expression expression = column.getExpression();
                    Optional<Identifier> field = column.getAlias();

                    Optional<QualifiedObjectName> originTable = Optional.empty();
                    QualifiedName name = null;

                    if (expression instanceof Identifier) {
                        name = QualifiedName.of(((Identifier) expression).getValue());
                    }
                    else if (expression instanceof DereferenceExpression) {
                        name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
                    }

                    if (name != null) {
                        List<Field> matchingFields = sourceScope.getRelationType().resolveFields(name);
                        if (!matchingFields.isEmpty()) {
                            originTable = matchingFields.get(0).getOriginTable();
                        }
                    }

                    if (!field.isPresent()) {
                        if (name != null) {
                            field = Optional.of(new Identifier(getLast(name.getOriginalParts())));
                        }
                    }
                    outputFields.add(Field.newUnqualified(field.map(Identifier::getValue), UNKNOWN, originTable, column.getAlias().isPresent()));
                }
                else {
                    throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            return createAndAssignScope(node, scope, outputFields.build());
        }

        private Scope createScope(Optional<Scope> parentScope)
        {
            return scopeBuilder(parentScope).build();
        }

        private Scope.Builder scopeBuilder(Optional<Scope> parentScope)
        {
            Scope.Builder scopeBuilder = Scope.builder();

            if (parentScope.isPresent()) {
                // parent scope represents local query scope hierarchy. Local query scope
                // hierarchy should have outer query scope as ancestor already.
                scopeBuilder.withParent(parentScope.get());
            }
            else if (outerQueryScope.isPresent()) {
                scopeBuilder.withOuterQueryParent(outerQueryScope.get());
            }

            return scopeBuilder;
        }

        private boolean hasScopeAsLocalParent(Scope root, Scope parent)
        {
            Scope scope = root;
            while (scope.getLocalParent().isPresent()) {
                scope = scope.getLocalParent().get();
                if (scope.equals(parent)) {
                    return true;
                }
            }

            return false;
        }
    }
}
