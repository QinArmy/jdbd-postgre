package io.jdbd.postgre;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
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


    public static ResultState assertUpdateOneWithMoreResult(ResultState state) {
        assertEquals(state.getAffectedRows(), 1L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertFalse(state.hasReturningColumn(), "returningColumn");
        assertTrue(state.hasMoreResult(), "moreResult");

        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }

    public static ResultState assertUpdateOneWithoutMoreResult(ResultState state) {
        assertEquals(state.getAffectedRows(), 1L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertFalse(state.hasReturningColumn(), "returningColumn");
        assertFalse(state.hasMoreResult(), "moreResult");

        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }

    public static ResultState assertUpdateOneAndReturningWithMoreResult(ResultState state) {
        assertEquals(state.getAffectedRows(), 1L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertTrue(state.hasReturningColumn(), "returningColumn");
        assertTrue(state.hasMoreResult(), "moreResult");

        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }

    public static ResultState assertUpdateOneAndReturningWithoutMoreResult(ResultState state) {
        assertEquals(state.getAffectedRows(), 1L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertTrue(state.hasReturningColumn(), "returningColumn");
        assertFalse(state.hasMoreResult(), "moreResult");

        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }

    public static ResultState assertQueryStateWithMoreResult(final ResultState state) {
        assertEquals(state.getAffectedRows(), 0L, "affectedRows");
        assertEquals(state.getInsertId(), 0L, "insertId");
        assertTrue(state.hasReturningColumn(), "returningColumn");
        assertTrue(state.hasMoreResult(), "moreResult");

        assertFalse(state.hasMoreFetch(), "moreFetch");
        return state;
    }


}
