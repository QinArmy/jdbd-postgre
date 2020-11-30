package io.jdbd;

import reactor.core.publisher.Flux;

public interface BatchUpdateResults {

    Flux<ResultStates> batchUpdate();

}
