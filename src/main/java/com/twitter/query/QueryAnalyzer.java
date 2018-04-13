package com.twitter.query;

import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Node;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
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

        @Override
        protected Scope visitNode(Node node, Optional<Scope> outerQueryScope)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }
    }

    // Utility Functions
    private static boolean hasScopeAsLocalParent(Scope root, Scope parent)
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
