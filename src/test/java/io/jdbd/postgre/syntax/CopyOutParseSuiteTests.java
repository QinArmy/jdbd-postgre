package io.jdbd.postgre.syntax;

import io.jdbd.postgre.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;

/**
 * @see PgParser#parseCopyOut(String)
 */
@Test(groups = {Group.COPY_OUT_PARSER}, dependsOnGroups = {Group.PARSER})
public class CopyOutParseSuiteTests {


    private static final Logger LOG = LoggerFactory.getLogger(CopyOutParseSuiteTests.class);


    /**
     * @see PgParser#parseCopyIn(String)
     */
    @Test
    public void copyOutToLocalFileWithTable() throws SQLException {
        final String fileName = "user.csv";
        String sql;
        final Consumer<CopyOut> consumer = copyOut -> {
            assertEquals(copyOut.getMode(), CopyOut.Mode.FILE, "CopyInMode");
            assertEquals(copyOut.getBindIndex(), -1, "bindIndex");
            assertEquals(copyOut.getPath().getFileName().toString(), fileName, "Copy in fileName");
        };
        // quote string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n TO  /* comment */\n '%s' ", fileName);

        doCopyOutParse(sql, consumer);
        // c-style string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n TO  /* comment */\n E'%s' ", fileName);

        doCopyOutParse(sql, consumer);

        // Unicode Escapes string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n TO  /* comment */\n U&'%s' ", fileName);

        doCopyOutParse(sql, consumer);

        // Dollar-Quoted string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n TO  /* comment */\n $tag$%s$tag$ ", fileName);

        doCopyOutParse(sql, consumer);
    }


    /**
     * @see PgParser#parseCopyIn(String)
     */
    @Test
    public void copyOutToLocalFileWithQuery() throws SQLException {
        final String fileName = "user.csv";
        String sql;
        final Consumer<CopyOut> consumer = copyOut -> {
            assertEquals(copyOut.getMode(), CopyOut.Mode.FILE, "CopyInMode");
            assertEquals(copyOut.getBindIndex(), -1, "bindIndex");
            assertEquals(copyOut.getPath().getFileName().toString(), fileName, "Copy in fileName");
        };
        final String query = "(SELECT u.id,u.\"user's name\",u.U&\"\\0441\\043B\\043E\\043D\" FROM user AS u WHERE u.id= ? (AND u.name = ? OR u.age = ?) AND u.nick_name= '(((()) ))))'  )";

        sql = String.format(
                "/* 'comment' */ \nCOPY /* comment */%s \n -- This is line comment \n TO  /* comment */\n '%s' ", query, fileName);

        doCopyOutParse(sql, consumer);
        // c-style string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY %s \n -- This is line comment \n TO  /* comment */\n E'%s' ", query, fileName);

        doCopyOutParse(sql, consumer);

        // Unicode Escapes string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY %s \n -- This is line comment \n TO  /* comment */\n U&'%s' ", query, fileName);

        doCopyOutParse(sql, consumer);

        // Dollar-Quoted string constant filename
        sql = String.format(
                "/* 'comment' */ \nCOPY %s \n -- This is line comment \n TO  /* comment */\n $tag$%s$tag$ ", query, fileName);

        doCopyOutParse(sql, consumer);
    }


    /**
     * @see PgParser#parseCopyIn(String)
     */
    @Test
    public void copyOutToLocalFileWithTableAndBind() throws SQLException {
        String sql;
        final Consumer<CopyOut> consumer = copyOut -> {
            assertEquals(copyOut.getMode(), CopyOut.Mode.FILE, "CopyInMode");
            assertEquals(copyOut.getBindIndex(), 0, "bindIndex");
        };
        // quote string constant filename
        sql = "/* 'comment' */ \nCOPY user(id,create_time,\"user's name\",U&\"\\0441\\043B\\043E\\043D\") \n -- This is line comment \n TO  /* comment */\n ? ";
        doCopyOutParse(sql, consumer);
    }


    /**
     * @see PgParser#parseCopyIn(String)
     */
    @Test
    public void copyOutToLocalFileWithQueryAndBind() throws SQLException {
        String sql;
        final Consumer<CopyOut> consumer = copyOut -> {
            assertEquals(copyOut.getMode(), CopyOut.Mode.FILE, "CopyInMode");
            assertEquals(copyOut.getBindIndex(), 3, "bindIndex");
        };
        final String query = "(SELECT u.id,u.\"user's name\",u.U&\"\\0441\\043B\\043E\\043D\" FROM user AS u WHERE u.id= ? (AND u.name = ? OR u.age = ?) AND u.nick_name= '(((()) ))))'  )";

        sql = String.format("/* 'comment' */ \nCOPY /* comment */%s \n -- This is line comment \n TO  /* comment */\n ? ", query);

        doCopyOutParse(sql, consumer);
    }


    private void doCopyOutParse(String sql, Consumer<CopyOut> consumer) throws SQLException {
        for (PgParser parser : PgParserSuiteTests.PARSER_LIST) {
            consumer.accept(parser.parseCopyOut(sql));
        }
    }


}
