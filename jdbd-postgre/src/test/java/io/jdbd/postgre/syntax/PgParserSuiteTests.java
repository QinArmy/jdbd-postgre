package io.jdbd.postgre.syntax;

import io.jdbd.postgre.Group;
import io.jdbd.postgre.ServerParameter;
import io.jdbd.postgre.util.PgArrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

/**
 * <p>
 * This class is suite test class of {@link PgParser}.
 * </p>
 *
 * @see PgParser
 */
@Test(groups = {Group.PARSER})
public class PgParserSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgParserSuiteTests.class);

    private static final PgParser PARSER_WITH_ON = PgParser.create(PgParserSuiteTests::standardConformingOnFunction);

    private static final PgParser PARSER_WITH_OFF = PgParser.create(PgParserSuiteTests::standardConformingOffFunction);

    static final List<PgParser> PARSER_LIST = PgArrays.asUnmodifiableList(PARSER_WITH_ON, PARSER_WITH_OFF);


    @Test
    public void simpleBlockComment() throws SQLException {
        LOG.info("simpleBlockComment test start ");
        staticSqlTest("/** * This is block comment. * **/");
        staticSqlTest("/** * This is block comment. * **/ /** this is second block comment.**/");
        LOG.info("simpleBlockComment test success");
    }

    @Test
    public void nestBlockComment() throws SQLException {
        LOG.info("nestBlockComment test start ");
        staticSqlTest("/** *-- This is /* /*nest*/ /*block*/ comment */. * **/");
        LOG.info("nestBlockComment test success");
    }

    @Test
    public void lineComment() throws SQLException {
        LOG.info("lineComment test start ");
        staticSqlTest("--This is line comment .");
        staticSqlTest("--This is line /* comment */ .");
        LOG.info("lineComment test success");
    }

    @Test(expectedExceptions = SQLException.class)
    public void notCloseSimpleBlockCommentError() throws SQLException {
        LOG.info("notCloseSimpleBlockCommentError test start ");
        try {
            staticSqlTest("/** * This is block comment. * **");
            fail("notCloseSimpleBlockCommentError test failure.");
        } catch (SQLException e) {
            LOG.info("notCloseSimpleBlockCommentError test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void notCloseNestBlockCommentError() throws SQLException {
        LOG.info("notCloseNestBlockCommentError test start ");
        try {
            staticSqlTest("/** * This is /*block comment. * **/");
            fail("notCloseNestBlockCommentError test failure.");
        } catch (SQLException e) {
            LOG.info("notCloseNestBlockCommentError test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test
    public void fullTest() throws SQLException {
        String sql, staticSql;

        String static0 = "/** Army persistence framework **/ --Army of QinArmy.\n"
                + " SELECT u.id,\"u\".U&\"n\\0061me\" "
                + " FROM U&\"\\0075\\0073\\0065\\0072\" UESCAPE '!' as u "
                + " WHERE u.id= ";
        String static1 = " AND u.name = 'user''name' AND u.nick_name = E'user\\'nickName' and u.book = &tag& book of Army &tag& ";

        sql = static0 + "?" + static1;

        PgStatement stmt;
        List<String> list;

        for (PgParser parser : PARSER_LIST) {
            stmt = parser.parse(sql);

            assertNotNull(stmt, "stmt");
            list = stmt.sqlPartList();
            assertNotNull(list, "list");
            assertEquals(list.size(), 2, "list size");

            staticSql = list.get(0);
            assertEquals(staticSql, static0);
            staticSql = list.get(1);
            assertEquals(staticSql, static1);

            assertEquals(stmt.getParamCount(), 1);
            assertEquals(stmt.getSql(), sql, "getSql()");
        }

    }

    @Test(expectedExceptions = SQLException.class)
    public void multipleStatementError() throws SQLException {
        LOG.info("multipleStatementError test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM user AS u WHERE u.id = 1 ; UPDATE user AS u SET u.name = 'Army' WHERE u.id = 3";
            staticSqlTest(sql);
            fail("multipleStatementError test failure.");
        } catch (SQLException e) {
            LOG.info("multipleStatementError test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void quoteStringNotCloseError1() throws SQLException {
        LOG.info("quoteStringNotCloseError test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM user AS u WHERE u.name = 'Army''class ";
            staticSqlTest(sql);
            fail("quoteStringNotCloseError test failure.");
        } catch (SQLException e) {
            LOG.info("quoteStringNotCloseError test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void quoteStringNotCloseError2() throws SQLException {
        LOG.info("quoteStringNotCloseError2 test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM user AS u WHERE u.name = 'Army ";
            staticSqlTest(sql);
            fail("quoteStringNotCloseError2 test failure.");
        } catch (SQLException e) {
            LOG.info("quoteStringNotCloseError2 test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void cStyleStringNotClose1() throws SQLException {
        LOG.info("cStyleStringNotClose1 test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM user AS u WHERE u.name = E'Army ";
            staticSqlTest(sql);
            fail("cStyleStringNotClose1 test failure.");
        } catch (SQLException e) {
            LOG.info("cStyleStringNotClose1 test success,message : {}", e.getMessage());
            throw e;
        }
    }


    @Test(expectedExceptions = SQLException.class)
    public void cStyleStringNotClose2() throws SQLException {
        LOG.info("cStyleStringNotClose2 test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM user AS u WHERE u.name = E'Army''class ";
            staticSqlTest(sql);
            fail("cStyleStringNotClose2 test failure.");
        } catch (SQLException e) {
            LOG.info("cStyleStringNotClose2 test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void cStyleStringNotClose3() throws SQLException {
        LOG.info("cStyleStringNotClose3 test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM user AS u WHERE u.name = E'Army\\'class ";
            staticSqlTest(sql);
            fail("cStyleStringNotClose3 test failure.");
        } catch (SQLException e) {
            LOG.info("cStyleStringNotClose3 test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void unicodeStringNotClose1() throws SQLException {
        LOG.info("unicodeStringNotClose1 test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM user AS u WHERE u.name = U&'Army ";
            staticSqlTest(sql);
            fail("unicodeStringNotClose1 test failure.");
        } catch (SQLException e) {
            LOG.info("unicodeStringNotClose1 test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void dollarStringNotClose() throws SQLException {
        LOG.info("dollarStringNotClose test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM user AS u WHERE u.name = $tag$Army$t";
            staticSqlTest(sql);
            fail("dollarStringNotClose test failure.");
        } catch (SQLException e) {
            LOG.info("dollarStringNotClose test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void unicodeStringNotClose2() throws SQLException {
        LOG.info("unicodeStringNotClose2 test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM user AS u WHERE u.name = U&'Army\\' ";
            staticSqlTest(sql);
            fail("unicodeStringNotClose2 test failure.");
        } catch (SQLException e) {
            LOG.info("unicodeStringNotClose2 test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void identifierNotClose() throws SQLException {
        LOG.info("identifierNotClose test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM \"user AS u WHERE u.name = 'Army' ";
            staticSqlTest(sql);
            fail("identifierNotClose test failure.");
        } catch (SQLException e) {
            LOG.info("identifierNotClose test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SQLException.class)
    public void unicodeIdentifierNotClose() throws SQLException {
        LOG.info("unicodeIdentifierNotClose test start ");
        try {
            String sql = "SELECT u.id ,u.name FROM U&\"user AS u WHERE u.name = 'Army' ";
            staticSqlTest(sql);
            fail("unicodeIdentifierNotClose test failure.");
        } catch (SQLException e) {
            LOG.info("unicodeIdentifierNotClose test success,message : {}", e.getMessage());
            throw e;
        }
    }

    @Test
    public void uescapeClauseErrorEscapes() {
        //TODO zoro 测试 UESCAPE '\' 和 UESCAPE ''' 的静态 sql 的解析
        //TODO zoro 测试 UESCAPE '\' 和 UESCAPE ''' 的 bindable sql 的 string parameter 解析 .
    }


    /**
     * @see PgParser#separateMultiStmt(String)
     */
    @Test
    public void separateMultiStmt() throws SQLException {
        final List<String> sqlGroup = new ArrayList<>();

        sqlGroup.add("/** * This is block comment. * **/");
        sqlGroup.add("/** *-- This is /* /*nest*/ /*block*/ comment */. * **/");
        sqlGroup.add(" SELECT u.id,\"u\".U&\"n\\0061me\" /* comment ; */ FROM U&\"\\0075\\0073\\0065\\0072\" UESCAPE '!' as u WHERE u.id=  AND u.name = 'user''name' AND u.nick_name = E'user\\'nickName' and u.book = &tag& book of Army &tag& ");
        sqlGroup.add("UPDATE user AS u SET name =  ';;' --COMMENT\n WHERE u.id = ?");

        StringBuilder builder = new StringBuilder();
        int count = 0;

        for (String sql : sqlGroup) {
            if (count > 0) {
                builder.append(";");
            }
            builder.append(sql);
            count++;
        }

        final String multiStmt = builder.toString();

        for (PgParser parser : PARSER_LIST) {
            final List<String> separatedGroup = parser.separateMultiStmt(multiStmt);
            assertEquals(separatedGroup.size(), sqlGroup.size(), "separatedGroup.size()");
            final int size = separatedGroup.size();
            for (int i = 0; i < size; i++) {
                assertEquals(separatedGroup.get(i), sqlGroup.get(i), sqlGroup.get(i));
            }

        }

    }


    /**
     * @see #simpleBlockComment()
     */
    private void staticSqlTest(final String sql) throws SQLException {
        String staticSql;
        PgStatement stmt;
        List<String> list;

        for (PgParser parser : PARSER_LIST) {
            stmt = parser.parse(sql);

            assertNotNull(stmt, "stmt");
            list = stmt.sqlPartList();
            assertNotNull(list, "list");
            assertEquals(list.size(), 1, "list size");

            staticSql = list.get(0);
            assertEquals(staticSql, sql);
            assertEquals(stmt.getParamCount(), 0);
            assertEquals(stmt.getSql(), sql, "getSql()");
        }

    }



    /*################################## blow private static method ##################################*/


    private static String standardConformingOnFunction(ServerParameter sp) {
        if (sp != ServerParameter.standard_conforming_strings) {
            throw new IllegalArgumentException(String.format("ServerParameter[%s] unsupported ", sp));
        }
        return "on";
    }

    private static String standardConformingOffFunction(ServerParameter sp) {
        if (sp != ServerParameter.standard_conforming_strings) {
            throw new IllegalArgumentException(String.format("ServerParameter[%s] unsupported ", sp));
        }
        return "off";
    }


}
