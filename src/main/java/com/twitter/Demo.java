package com.twitter;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.security.Identity;
import com.twitter.query.Console;
import com.twitter.query.QueryAnalysis;
import com.twitter.query.oline.QueryAnalyzer;

import java.util.Optional;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

/**
 * Hello world!
 */
public class Demo
{
    public static void main(String[] args)
    {
//        // parse input string
//        if (args.length != 2) {
//            System.out.println("Usage : ./analyzer <catalog> <schema>");
//            return;
//        }
//
//        QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
//        Session session = Session.builder(new SessionPropertyManager())
//                .setQueryId(queryIdGenerator.createNextQueryId())
//                .setIdentity(new Identity("user", Optional.empty()))
//                .setCatalog(args[0])
//                .setSchema(args[1])
//                .setTimeZoneKey(UTC_KEY)
//                .setLocale(ENGLISH)
//                .build();
//
//        Console console = new Console(session);
//        System.exit(console.run() ? 0 : 1);

        runTest();
    }

    public static void runTest()
    {
        System.out.println("Hello, world!");
        try {
            //String query = "select * from x join y on x.a = y.b where x.a > 0 and y.b < 0 ";
            //String query = "select c, count(*) from x where a > 0 and b < 0  group by c";
            String query = "show tables";
            QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
            Session session = Session.builder(new SessionPropertyManager())
                    .setQueryId(queryIdGenerator.createNextQueryId())
                    .setIdentity(new Identity("user", Optional.empty()))
                    .setCatalog("dal")
                    .setSchema("datapipeline")
                    .setTimeZoneKey(UTC_KEY)
                    .setLocale(ENGLISH)
                    .build();

            QueryAnalyzer analyzer = new QueryAnalyzer(session);
            QueryAnalysis analysis = analyzer.analyze(query);
            analysis.report();
            analyzer.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {

        }
    }
}
