package io.jdbd.postgre;

import io.jdbd.result.ResultState;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

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


}
