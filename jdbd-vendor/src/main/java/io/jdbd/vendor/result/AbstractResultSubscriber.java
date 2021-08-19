package io.jdbd.vendor.result;

import io.jdbd.ResultStatusConsumerException;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import org.reactivestreams.Subscription;
import reactor.core.publisher.FluxSink;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

abstract class AbstractResultSubscriber implements ResultSubscriber {

    List<Throwable> errorList;

    Subscription subscription;

    AbstractResultSubscriber() {
    }


    abstract ResultType getSubscribeType();

    /**
     * @return true : stateConsumer throw error
     */
    final boolean fluxSinkComplete(FluxSink<ResultRow> sink, Consumer<ResultState> stateConsumer, ResultState state) {
        Throwable consumerError = null;
        try {
            stateConsumer.accept(state);
        } catch (Throwable e) {
            consumerError = e;
        }
        if (consumerError == null) {
            sink.complete();
        } else {
            ResultStatusConsumerException re = ResultStatusConsumerException.create(stateConsumer, consumerError);
            addError(re);
            sink.error(re);
        }
        return consumerError != null;
    }

    final void addSubscribeError(ResultType resultType) {
        List<Throwable> errorList = this.errorList;
        if (errorList == null || errorList.isEmpty()) {
            addError(new SubscribeException(getSubscribeType(), resultType));
            return;
        }
        boolean add = true;
        switch (resultType) {
            case UPDATE:
            case BATCH_UPDATE:
            case QUERY: {
                for (Throwable e : errorList) {
                    if (e.getClass() == SubscribeException.class) {
                        add = false;
                        break;
                    }
                }
            }
            break;
            case MULTI_RESULT: {
                final int size = errorList.size();
                for (int i = 0; i < size; i++) {
                    Throwable e = errorList.get(i);
                    if (e.getClass() != SubscribeException.class) {
                        continue;
                    }
                    SubscribeException se = (SubscribeException) e;
                    if (se.getActualType() != ResultType.MULTI_RESULT) {
                        errorList.set(i, new SubscribeException(getSubscribeType(), ResultType.MULTI_RESULT));
                    }
                    add = false;
                    break;
                }
            }
            break;
            default:
                throw new IllegalArgumentException("resultType error.");
        }

        if (add) {
            addError(new SubscribeException(getSubscribeType(), resultType));
        }
    }


    final void addError(Throwable error) {
        List<Throwable> errorList = this.errorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
            this.errorList = errorList;
            final Subscription s = this.subscription;
            if (s != null) {
                s.cancel();
            }
        }
        errorList.add(error);
    }

    final boolean hasError() {
        List<Throwable> errorList = this.errorList;
        return !(errorList == null || errorList.isEmpty());
    }


    static IllegalArgumentException createUnknownTypeError(Result result) {
        return new IllegalArgumentException(String.format("Unknown type[%s]", result.getClass().getName()));
    }

    static IllegalArgumentException createDuplicationResultState(ResultState state) {
        return new IllegalArgumentException(String.format("ResultState [%s] duplication", state.getClass().getName()));
    }

}
