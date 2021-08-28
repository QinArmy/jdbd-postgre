package io.jdbd.postgre.syntax;

import io.jdbd.postgre.Group;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.vendor.stmt.Stmt;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;

/**
 * <p>
 * This class is suite test class of {@link PgParser#parseCopyIn(Stmt, int)}.
 * </p>
 *
 * @see PgParser#parseCopyIn(Stmt, int)
 */
@Test(groups = {Group.COPY_IN_PARSER}, dependsOnGroups = {Group.PARSER})
public class PgCopyInParseSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgCopyInParseSuiteTests.class);


    /**
     * @see PgParser#parseCopyIn(Stmt, int)
     */
    @Test
    public void copyInWithFileName() throws SQLException {
        final String fileName = "user.csv";
        String sql;
        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.FILE, "CopyInMode");
            assertEquals(copyIn.getPath().getFileName().toString(), fileName, "Copy in fileName");
        };
        // quote string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n '%s' ", fileName);

        doCopyInTests(PgStmts.stmt(sql), consumer);
        // c-style string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n E'%s' ", fileName);

        doCopyInTests(PgStmts.stmt(sql), consumer);

        // Unicode Escapes string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n U&'%s' ", fileName);

        doCopyInTests(PgStmts.stmt(sql), consumer);

        // Dollar-Quoted string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n $tag$%s$tag$ ", fileName);

        doCopyInTests(PgStmts.stmt(sql), consumer);
    }

    /**
     * @see PgParser#parseCopyIn(Stmt, int)
     */
    @Test
    public void copyInWithBindFileName() throws SQLException {
        final String fileName = Paths.get(System.getProperty("user.home"), "user.csv").toAbsolutePath().toString();
        final String sql = "/* comment */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n ? ";

        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.FILE, "CopyInMode");
            assertEquals(copyIn.getPath().toAbsolutePath().toString(), fileName, "Copy in fileName");
        };
        doCopyInTests(PgStmts.bindable(sql, BindValue.create(0, PgType.VARCHAR, fileName)), consumer);
    }

    /**
     * @see PgParser#parseCopyIn(Stmt, int)
     */
    @Test
    public void copyInWithProgramCommand() throws SQLException {
        final String command = "SHELL COMMAND";

        final Function<String, Publisher<byte[]>> function = cmd -> noActionPublisher();

        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.PROGRAM, "CopyInMode");
            assertEquals(copyIn.getCommand(), command, "Copy in command");
            assertEquals(copyIn.getFunction(), function, "import function");
        };

        String sql;

        // quote string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n PROGRAM --comment\n'%s' ", command);

        doCopyInTests(PgStmts.stmtWithImport(sql, function), consumer);
        // c-style string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n  PROGRAM --comment\nE'%s' ", command);

        doCopyInTests(PgStmts.stmtWithImport(sql, function), consumer);

        // Unicode Escapes string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n  PROGRAM --comment\nU&'%s' ", command);

        doCopyInTests(PgStmts.stmtWithImport(sql, function), consumer);

        // Dollar-Quoted string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment */\n  PROGRAM --comment\n$tag$%s$tag$ ", command);

        doCopyInTests(PgStmts.stmtWithImport(sql, function), consumer);
    }

    /**
     * @see PgParser#parseCopyIn(Stmt, int)
     */
    @Test
    public void copyInWithBindProgramCommand() throws SQLException {
        final String command = "SHELL COMMAND";

        final Function<String, Publisher<byte[]>> function = cmd -> noActionPublisher();
        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.PROGRAM, "CopyInMode");
            assertEquals(copyIn.getCommand(), command, "Copy in command");
            assertEquals(copyIn.getFunction(), function, "import function");
        };

        final String sql = "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n FROM  /* comment 'filename' */\n PROGRAM --comment\n ? ";

        final BindableStmt stmt = PgStmts.bindableWithImport(sql, BindValue.create(0, PgType.VARCHAR, command), function);
        doCopyInTests(stmt, consumer);
    }

    /**
     * @see PgParser#parseCopyIn(Stmt, int)
     */
    @Test
    public void copyInWIthStdin() throws SQLException {

        final Function<String, Publisher<byte[]>> function = cmd -> noActionPublisher();
        final Consumer<CopyIn> consumer = copyIn -> {
            assertEquals(copyIn.getMode(), CopyIn.Mode.STDIN, "CopyInMode");
            assertEquals(copyIn.getFunction(), function, "import function");
        };
        final String sql = "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment  \n FROM  /* comment  */\n STDIN --comment\n  ";

        doCopyInTests(PgStmts.stmtWithImport(sql, function), consumer);
    }


    private void doCopyInTests(Stmt stmt, Consumer<CopyIn> consumer) throws SQLException {
        for (PgParser parser : PgParserSuiteTests.PARSER_LIST) {
            consumer.accept(parser.parseCopyIn(stmt, 0));
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
