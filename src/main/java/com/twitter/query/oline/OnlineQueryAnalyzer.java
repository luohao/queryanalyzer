package com.twitter.query.oline;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.hive.HiveConnector;
import com.facebook.presto.hive.HiveConnectorFactory;
import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HiveSchemaProperties;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.query.QueryAnalysis;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.connector.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.connector.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.transaction.TransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.Locale.ENGLISH;

public class OnlineQueryAnalyzer
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final String HMS_URI = "thrift://hadoop-master:9083";

    // MetadataManager related stuff
    private Session session;
    private CatalogManager catalogManager;
    private TransactionManager transactionManager;
    private AccessControl accessControl;

    public OnlineQueryAnalyzer()
    {
        createSession();
        catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AccessControlManager(transactionManager);
    }

    public QueryAnalysis analyze(String query)
    {
        Statement statement = SQL_PARSER.createStatement(query);
        QueryAnalysis queryAnalysis = new QueryAnalysis(statement);
        Metadata metadata = createMetadata();

        Connector connector = createHiveConnector(HMS_URI);

        ConnectorTransactionHandle txn = connector.beginTransaction(READ_UNCOMMITTED, true);
        connector.getMetadata(txn);
        connector.commit(txn);

        //registerHiveMetastore("hive", HMS_URI);
//        transaction(transactionManager, accessControl)
//                .singleStatement()
//                .readCommitted()
//                .readOnly()
//                .execute(session, session -> {
//                    Analyzer analyzer = new Analyzer(session, metadata, SQL_PARSER, accessControl, Optional.empty(), emptyList());
//                    Analysis analysis = analyzer.analyze(statement);
//                });
        return queryAnalysis;
    }

    // Utility functions
    private Metadata createMetadata()
    {
        //return createTestMetadataManager(catalogManager);
        TypeManager typeManager = new TypeRegistry();
        return new MetadataManager(
                new FeaturesConfig(),
                typeManager,
                new BlockEncodingManager(typeManager),
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                transactionManager);
    }

    private void createSession()
    {
        session = Session.builder(new SessionPropertyManager())
                .setQueryId((new QueryIdGenerator()).createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty()))
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .setRemoteUserAddress("address")
                .setUserAgent("agent")
                .setCatalog("test_schema")
                .setSchema("s1")
                .build();
    }

    private void registerHiveMetastore(String catalogName, String metastoreUri)
    {
        ConnectorId connectorId = new ConnectorId(catalogName);
        Connector connector = createHiveConnector(metastoreUri);

        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        MetadataManager metadata = MetadataManager.createTestMetadataManager(catalogManager);
        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        catalogManager.registerCatalog(new Catalog(
                catalogName,
                connectorId,
                connector,
                createInformationSchemaConnectorId(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
                systemId,
                new SystemConnector(
                        systemId,
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId))));
    }

    private Connector createHiveConnector(String metastoreUri)
    {
        HiveConnectorFactory connectorFactory = new HiveConnectorFactory(
                "hive",
                HiveConnector.class.getClassLoader(),
                null);

        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", metastoreUri)
                .build();

        //Connector connector = connectorFactory.create("hive", config, new TestingConnectorContext());
        Connector connector = new HiveConnector(
                null,
                new HiveMetadataFactory(),
                transactionManager,
                null,
                null,
                null,
                ImmutableSet.of(),
                emptyList(),
                HiveSchemaProperties.SCHEMA_PROPERTIES,
                emptyList(),
                accessControl,
                null
                );
        return connector;
    }
/*
    private static ConnectorId registerBogusConnector(CatalogManager catalogManager, TransactionManager transactionManager, AccessControl accessControl, String catalogName)
    {
        ConnectorId connectorId = new ConnectorId(catalogName);
        Connector connector = new TpchConnectorFactory().create(catalogName, ImmutableMap.of(), new TestingConnectorContext());

        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        MetadataManager metadata = MetadataManager.createTestMetadataManager(catalogManager);
        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        catalogManager.registerCatalog(new Catalog(
                catalogName,
                connectorId,
                connector,
                createInformationSchemaConnectorId(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
                systemId,
                new SystemConnector(
                        systemId,
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId))));

        return connectorId;
    }
    */
}
