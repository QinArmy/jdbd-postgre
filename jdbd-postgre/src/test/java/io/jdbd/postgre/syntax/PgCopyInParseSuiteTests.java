package io.jdbd.postgre.syntax;

import io.jdbd.postgre.Group;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

/**
 * <p>
 * This class is suite test class of {@link PgParser#parseCopyIn(String)}.
 * </p>
 *
 * @see PgParser#parseCopyIn(String)
 */
@Test(groups = {Group.COPY_IN_PARSER}, dependsOnGroups = {Group.PARSER})
public class PgCopyInParseSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgCopyInParseSuiteTests.class);


    /**
     * @see PgParser#parseCopyIn(String)
     */
    @Test
    public void copyInWithFileName() throws SQLException {
        final String fileName = "user.csv";
        String sql;
        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.FILE, "CopyInMode");
            assertEquals(copyIn.getBindIndex(), -1, "bindIndex");
            assertEquals(copyIn.getPath().getFileName().toString(), fileName, "Copy in fileName");
        };
        // quote string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n '%s' ", fileName);

        doCopyInTests(sql, consumer);
        // c-style string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n E'%s' ", fileName);

        doCopyInTests(sql, consumer);

        // Unicode Escapes string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n U&'%s' ", fileName);

        doCopyInTests(sql, consumer);

        // Dollar-Quoted string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n $tag$%s$tag$ ", fileName);

        doCopyInTests(sql, consumer);
    }

    /**
     * @see PgParser#parseCopyIn(String)
     */
    @Test
    public void copyInWithBindFileName() throws SQLException {
        final String sql = "/* comment */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n ? ";

        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.FILE, "CopyInMode");
            assertEquals(copyIn.getBindIndex(), 0, "bindIndex");
        };
        doCopyInTests(sql, consumer);
    }

    /**
     * @see PgParser#parseCopyIn(String)
     */
    @Test(enabled = false)
    public void copyInWithProgramCommand() throws SQLException {
        final String command = "SHELL COMMAND";

        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.PROGRAM, "CopyInMode");
            assertEquals(copyIn.getCommand(), command, "Copy in command");
        };

        String sql;

        // quote string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n PROGRAM --comment\n'%s' ", command);

        doCopyInTests(sql, consumer);
        // c-style string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n  PROGRAM --comment\nE'%s' ", command);

        doCopyInTests(sql, consumer);

        // Unicode Escapes string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n  PROGRAM --comment\nU&'%s' ", command);

        doCopyInTests(sql, consumer);

        // Dollar-Quoted string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n  PROGRAM --comment\n$tag$%s$tag$ ", command);

        doCopyInTests(sql, consumer);
    }

    /**
     * @see PgParser#parseCopyIn(String)
     */
    @Test(enabled = false)
    public void copyInWithBindProgramCommand() throws SQLException {
        final String command = "SHELL COMMAND";

        final Function<String, Publisher<byte[]>> function = cmd -> noActionPublisher();
        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.PROGRAM, "CopyInMode");
            assertEquals(copyIn.getCommand(), command, "Copy in command");
        };

        final String sql = "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment 'filename' */\n PROGRAM --comment\n ? ";

        doCopyInTests(sql, consumer);
    }

    /**
     * @see PgParser#parseCopyIn(String)
     */
    @Test(enabled = false)
    public void copyInWIthStdin() throws SQLException {

        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.STDIN, "CopyInMode");
        };
        final String sql = "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment  \n FROM  /* comment  */\n STDIN --comment\n  ";

        doCopyInTests(sql, consumer);
    }


    private void doCopyInTests(String sql, Consumer<CopyIn> consumer) throws SQLException {
        for (PgParser parser : PgParserSuiteTests.PARSER_LIST) {
            consumer.accept(parser.parseCopyIn(sql));
        }
    }


    @SuppressWarnings("unchecked")
    private static <T> Publisher<T> noActionPublisher() {
        return (Publisher<T>) EmptyPublisher.INSTANCE;
    }

    @SuppressWarnings("all")
    private static final class EmptyPublisher<T> implements Publisher<T> {

        private static final EmptyPublisher<?> INSTANCE = new EmptyPublisher<>();

        @Override
        public void subscribe(Subscriber<? super T> s) {
            // no-ope
        }

    }


}
