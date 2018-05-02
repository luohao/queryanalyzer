package com.twitter.query;

import com.facebook.presto.Session;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.twitter.query.oline.QueryAnalyzer;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;

import static com.facebook.presto.sql.parser.StatementSplitter.squeezeStatement;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static java.util.Locale.ENGLISH;

public class Console
{
    Session session;
    QueryAnalyzer analyzer;
    private static final String PROMPT_NAME = "analyzer";

    public Console(Session session)
    {
        try {
            this.session = session;
            this.analyzer = new QueryAnalyzer(session);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean run()
    {
        initializeLogging();
        runConsole(session);
        return true;
    }

    private class QueryAnalyzerException
            extends Exception
    {
        public QueryAnalyzerException(String message)
        {
            super(message);
        }
    }

    private class LineReader
            extends ConsoleReader
            implements Closeable
    {
        private boolean interrupted;

        LineReader()
                throws IOException
        {
            setExpandEvents(false);
            setBellEnabled(true);
            setHandleUserInterrupt(true);
            setHistoryEnabled(false);
        }

        @Override
        public String readLine(String prompt, Character mask)
                throws IOException
        {
            String line;
            interrupted = false;
            try {
                line = super.readLine(prompt, mask);
            }
            catch (UserInterruptException e) {
                interrupted = true;
                return null;
            }

            return line;
        }

        @Override
        public void close()
        {
            shutdown();
        }

        public boolean interrupted()
        {
            return interrupted;
        }
    }

    private void runConsole(Session session)
    {
        try {
            StringBuilder buffer = new StringBuilder();
            LineReader reader = new LineReader();

            session.getCatalog().orElseThrow(() -> new QueryAnalyzerException("Catalog must be set"));
            session.getSchema().orElseThrow(() -> new QueryAnalyzerException("Schema must be set"));
            while (true) {
                // read a line of input from user
                String schema = session.getSchema().get();
                String prompt = PROMPT_NAME + ":" + schema;

                if (buffer.length() > 0) {
                    prompt = Strings.repeat(" ", prompt.length() - 1) + "-";
                }
                String commandPrompt = prompt + "> ";
                String line = reader.readLine(commandPrompt);

                // add buffer to history and clear on user interrupt
                if (reader.interrupted()) {
                    String partial = squeezeStatement(buffer.toString());
                    if (!partial.isEmpty()) {
                        reader.getHistory().add(partial);
                    }
                    buffer = new StringBuilder();
                    continue;
                }

                // exit on EOF
                if (line == null) {
                    System.out.println();
                    return;
                }

                if (buffer.length() == 0) {
                    String command = line.trim();

                    if (command.endsWith(";")) {
                        command = command.substring(0, command.length() - 1).trim();
                    }

                    switch (command.toLowerCase(ENGLISH)) {
                        case "exit":
                        case "quit":
                            return;
                    }
                }

                // not a command, add line to buffer
                buffer.append(line).append("\n");

                // execute any complete statements
                String sql = buffer.toString();

                StatementSplitter splitter = new StatementSplitter(sql, ImmutableSet.of(";", "\\G"));
                for (StatementSplitter.Statement split : splitter.getCompleteStatements()) {
                    // analyze the query
                    QueryAnalysis result = analyzer.analyze(split.statement());
                    result.report();
                }

                // replace buffer with trailing partial statement
                buffer = new StringBuilder();
                String partial = splitter.getPartialStatement();
                if (!partial.isEmpty()) {
                    buffer.append(partial).append('\n');
                }
            }
        }
        catch (Exception e) {
            System.err.println("Readline error: " + e.getMessage());
        }
    }

    private static void initializeLogging()
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;

        try {
            LoggingConfiguration config = new LoggingConfiguration();

            System.setOut(new PrintStream(nullOutputStream()));
            System.setErr(new PrintStream(nullOutputStream()));

            config.setConsoleEnabled(false);

            Logging logging = Logging.initialize();
            logging.configure(config);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }

}
