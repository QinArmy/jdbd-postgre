package io.jdbd.postgre.statement;

import io.jdbd.postgre.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Test(groups = {Group.STMT_TEST_ADVICE}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS, Group.SESSION_BUILDER
        , Group.TASK_TEST_ADVICE, Group.SIMPLE_QUERY_TASK, Group.EXTENDED_QUERY_TASK})
public class StatementTestAdvice extends AbstractStatementTests {

    private static final Logger LOG = LoggerFactory.getLogger(StatementTestAdvice.class);

    @AfterSuite
    public void afterSuite() {
        Flux.fromIterable(SESSION_QUEUE)
                .flatMap(session -> Mono.from(session.close()))
                .blockLast();
    }

    @Test
    public void none() {
        // no-op
    }

}
