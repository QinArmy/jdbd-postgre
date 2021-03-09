package io.jdbd.mysql.syntax;


import io.jdbd.mysql.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

@Test(groups = {Groups.SQL_PARSER}, dependsOnGroups = {Groups.MYSQL_URL})
public class MySQLParserSuiteTests {

    static final Logger LOG = LoggerFactory.getLogger(MySQLParserSuiteTests.class);


    @Test
    public void singleStmt() {
        LOG.info("singleStmt test start.");
    }
}
