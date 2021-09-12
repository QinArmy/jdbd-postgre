package io.jdbd.postgre;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.function.Function;

import static org.testng.Assert.*;

public abstract class PgTestUtils {

    public static <T> Mono<T> updateNoResponse() {
        return Mono.defer(() -> Mono.error(new RuntimeException("update no response")));
    }

    public static <T> Flux<T> queryNoResponse() {
        return Flux.defer(() -> Flux.error(new RuntimeException("update no response")));
    }

    public static <T> T mapListToOne(List<T> list) {
        if (list.size() != 1) {
            throw new RuntimeException(String.format("list size[%s] isn't one.", list.size()));
        }
        return list.get(0);
    }

    public static Function<ResultRow, ResultRow> assertRowIdFunction(final long id) {
        return row -> {
            assertEquals(row.get("id"), id, "id");
            return row;
        };
    }

    public static Function<ResultRow, ResultRow> assertColumnValue(String columnLabel, @Nullable Object bindValue) {
        return row -> {
            assertEquals(row.get(columnLabel), bindValue, columnLabel);
            return row;
        };
    }

    public static ResultStates assertUpdateOneWithMoreResult(ResultStates states, int resultIndex) {
        assertEquals(states.getResultIndex(), resultIndex, "resultIndex");
        return assertUpdateOneWithMoreResult(states);
    }

    public static ResultStates assertUpdateOneWithMoreResult(ResultStates state) {
        assertEquals(state.getAffectedRows(), 1L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertFalse(state.hasColumn(), "returningColumn");
        assertTrue(state.hasMoreResult(), "moreResult");

        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }

    public static ResultStates assertUpdateOneWithoutMoreResult(ResultStates states, int resultIndex) {
        assertEquals(states.getResultIndex(), resultIndex, "resultIndex");
        return assertUpdateOneWithoutMoreResult(states);
    }

    public static ResultStates assertUpdateOneWithoutMoreResult(ResultStates state) {
        assertEquals(state.getAffectedRows(), 1L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertFalse(state.hasColumn(), "returningColumn");
        assertFalse(state.hasMoreResult(), "moreResult");

        assertEquals(state.getRowCount(), 0L, "rowCount");
        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }

    public static ResultStates assertUpdateOneAndReturningWithMoreResult(ResultStates state) {
        assertEquals(state.getAffectedRows(), 1L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertTrue(state.hasColumn(), "returningColumn");
        assertTrue(state.hasMoreResult(), "moreResult");

        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }

    public static ResultStates assertUpdateOneAndReturningWithoutMoreResult(ResultStates state) {
        assertEquals(state.getAffectedRows(), 1L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertTrue(state.hasColumn(), "returningColumn");
        assertFalse(state.hasMoreResult(), "moreResult");

        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }

    public static ResultStates assertQueryStateWithMoreResult(final ResultStates state) {
        assertEquals(state.getAffectedRows(), 0L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertTrue(state.hasColumn(), "returningColumn");
        assertTrue(state.hasMoreResult(), "moreResult");

        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }


}
