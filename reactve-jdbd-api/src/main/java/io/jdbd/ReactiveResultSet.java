package io.jdbd;

import reactor.core.publisher.Flux;

public interface ReactiveResultSet {

    ResultRowMeta getRowMeta();

    Flux<ResultRow> rowFlux();

}
