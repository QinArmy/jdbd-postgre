package io.jdbd.mysql.protocol.client;


import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.MySQLBindValue;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.StmtWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * <p>
 * id range [51,99]
 * </p>
 *
 * @see ComPreparedTask
 */
@Test
//(groups = {Groups.COM_STMT_PREPARE}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS, Groups.DATA_PREPARE})
public class ComPreparedTaskSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComPreparedTaskSuiteTests.class);

    public ComPreparedTaskSuiteTests() {
        super(SubType.COM_PREPARE_STMT);
    }

    @Override
    Mono<ResultStates> executeUpdate(StmtWrapper wrapper, MySQLTaskAdjutant taskAdjutant) {
        return ComPreparedTask.update(wrapper, taskAdjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(StmtWrapper wrapper, MySQLTaskAdjutant taskAdjutant) {
        return ComPreparedTask.query(wrapper, taskAdjutant);
    }

    /**
     * @see ComPreparedTask#update(StmtWrapper, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void update() {
        LOG.info("prepare update test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        String sql;
        List<BindValue> bindValueList;
        ResultStates states;

        sql = "UPDATE mysql_types as t SET t.my_tiny_text = ? WHERE t.id = ?";
        bindValueList = new ArrayList<>(2);
        bindValueList.add(MySQLBindValue.create(0, MySQLType.TINYTEXT, "prepare update 1"));
        bindValueList.add(MySQLBindValue.create(1, MySQLType.BIGINT, 80L));

        states = ComPreparedTask.update(StmtWrappers.multi(sql, bindValueList), adjutant)
                .block();

        assertNotNull(states, "states");
        assertEquals(states.getAffectedRows(), 1L, "getAffectedRows");

        LOG.info("prepare update test success");
        releaseConnection(adjutant);
    }


}
