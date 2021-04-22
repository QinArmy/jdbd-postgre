package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.ResultStatusConsumerException;
import io.jdbd.result.ResultStatus;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import reactor.util.annotation.Nullable;

import java.util.function.Consumer;

/**
 * <p>
 * This class is util class of below tow Task:
 *     <ul>
 *         <li>{@link ComQueryTask}</li>
 *         <li>{@link ComPreparedTask}</li>
 *     </ul>
 * </p>
 */
abstract class TaskUtils {

    private TaskUtils() {
        throw new UnsupportedOperationException();
    }


    @Nullable
    static SubscribeException replaceAsQueryMultiError(final JdbdException e) {
        SubscribeException newError = null;
        if (e instanceof SubscribeException) {
            SubscribeException se = (SubscribeException) e;
            if (se.getSubscribeType() != ResultType.QUERY || se.getActualType() != ResultType.MULTI_RESULT) {
                newError = new SubscribeException(ResultType.QUERY, ResultType.MULTI_RESULT);
            }
        }
        return newError;
    }

    @Nullable
    static SubscribeException replaceAsUpdateMultiError(final JdbdException e) {
        SubscribeException newError = null;
        if (e instanceof SubscribeException) {
            SubscribeException se = (SubscribeException) e;
            if (se.getSubscribeType() != ResultType.UPDATE || se.getActualType() != ResultType.MULTI_RESULT) {
                newError = createUpdateMultiError();
            }
        }
        return newError;
    }

    @Nullable
    static SubscribeException replaceAsBatchUpdateMultiError(final JdbdException e) {
        SubscribeException newError = null;
        if (e instanceof SubscribeException) {
            SubscribeException se = (SubscribeException) e;
            if (se.getSubscribeType() != ResultType.BATCH_UPDATE || se.getActualType() != ResultType.MULTI_RESULT) {
                newError = createBatchUpdateMultiError();
            }
        }
        return newError;
    }


    static SubscribeException createBatchUpdateQueryError() {
        return new SubscribeException(ResultType.BATCH_UPDATE, ResultType.QUERY);
    }

    static SubscribeException createBatchUpdateMultiError() {
        return new SubscribeException(ResultType.BATCH_UPDATE, ResultType.MULTI_RESULT);
    }

    static SubscribeException createUpdateMultiError() {
        return new SubscribeException(ResultType.UPDATE, ResultType.MULTI_RESULT);
    }

    static SubscribeException createUpdateQueryError() {
        return new SubscribeException(ResultType.UPDATE, ResultType.QUERY);
    }

    static SubscribeException createQueryMultiError() {
        return new SubscribeException(ResultType.QUERY, ResultType.MULTI_RESULT);
    }

    static SubscribeException createQueryUpdateError() {
        return new SubscribeException(ResultType.QUERY, ResultType.UPDATE);
    }

    static ResultStatusConsumerException createStateConsumerError(Throwable cause, Consumer<ResultStatus> consumer) {
        String message = String.format("%s consumer[%s] throw exception.", ResultStatus.class.getName(), consumer);
        return new ResultStatusConsumerException(message, cause);
    }


}
