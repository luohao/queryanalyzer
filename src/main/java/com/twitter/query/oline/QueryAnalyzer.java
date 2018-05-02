package com.twitter.query.oline;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.twitter.query.QueryAnalysis;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;

public class QueryAnalyzer
{
    public static final String HIVE_CATALOG = "hive";
    private static final SqlParser SQL_PARSER = new SqlParser();
    private final TestingPrestoServer server;
    private final Session session;

    public QueryAnalyzer(Session session)
            throws Exception
    {
        try {
            this.session = session;
            server = createTestingPrestoServer();
            registerHiveCatalog();
        }
        catch (Exception e) {
            close();
            throw e;
        }
    }

    public QueryAnalysis analyze(String query)
    {
        QueryAnalysis queryAnalysis = new QueryAnalysis(query);
        Run(session -> {
            Statement statement = SQL_PARSER.createStatement(query);
            Analyzer analyzer = new Analyzer(session,
                    server.getMetadata(),
                    SQL_PARSER,
                    server.getAccessControl(),
                    Optional.empty(),
                    Collections.emptyList());
            Analysis analysis = analyzer.analyze(statement);
            SqlAnalyzer sqlAnalyzer = new SqlAnalyzer(session, analysis, queryAnalysis);
            sqlAnalyzer.analyze(statement);
        });
        return queryAnalysis;
    }

    public void close()
    {
        try {
            server.close();
        }
        catch (Exception e) {
            // close quietly
        }
    }

    private static TestingPrestoServer createTestingPrestoServer()
            throws Exception
    {
        return new TestingPrestoServer(true, ImmutableMap.<String, String>builder().build(), null, null, new SqlParserOptions(), ImmutableList.of());
    }

    // register Hive catalog
    private void registerHiveCatalog()
    {
        // TODO: move catalog configs to files and load from config file
        server.installPlugin(new HivePlugin(HIVE_CATALOG, null));
        server.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", "thrift://hadoop-master:9083")
                .build());
    }

    // Run task in a transaction context, otherwise connector will complain.
    private void Run(Consumer<Session> task)
    {
        try {
            transaction(server.getTransactionManager(), server.getAccessControl())
                    .singleStatement()
                    .readUncommitted()
                    .readOnly()
                    .execute(session, task);
        }
        catch (Exception e) {
            //close();
            e.printStackTrace();
            throw e;
        }
    }

    // Pull column names for audit table from HMS in order to verify Hive catalog is installed
    public void testMetadata()
    {
        final String TABLE_NAME = "hive.default.audit";
        Metadata metadata = server.getMetadata();

        Run(session ->
        {
            System.out.println("Columns in table " + TABLE_NAME);
            QualifiedObjectName name = QualifiedObjectName.valueOf(TABLE_NAME);
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, name);
            if (tableHandle.isPresent()) {
                TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle.get());
                for (ColumnMetadata column : tableMetadata.getColumns()) {
                    System.out.println("    " + column.getName());
                }
            }
        });
    }

    public void test()
    {
        List<List<Integer>> testlists = new LinkedList<>();
        testlists.add(ImmutableList.of(1, 2, 3));
        testlists.add(ImmutableList.of(4, 5, 6));
        testlists.add(ImmutableList.of(7, 8, 9));

        List<Integer> lists = testlists.stream().flatMap(x -> x.stream()).collect(toImmutableList());
        System.out.println(lists);
    }

    public static Session createTestSession()
    {
        QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
        return Session.builder(new SessionPropertyManager())
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty()))
                .setCatalog("hive")
                .setSchema("default")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setRemoteUserAddress("address")
                .setUserAgent("agent")
                .build();
    }
}
