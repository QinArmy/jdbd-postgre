package io.jdbd.vendor.result;

import io.jdbd.result.SingleResult;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.task.ITaskAdjutant;
import reactor.core.CoreSubscriber;

import java.util.ArrayList;
import java.util.List;

abstract class AbstractSingleResultSubscriber implements CoreSubscriber<SingleResult> {

    final ITaskAdjutant adjutant;

    List<Throwable> errorList;

    AbstractSingleResultSubscriber(ITaskAdjutant adjutant) {
        this.adjutant = adjutant;
    }


    abstract ResultType getSubscribeType();

    final void addUpstreamError(Throwable error) {
        if (this.adjutant.inEventLoop()) {
            doAddErrorInEventLoop(error);
        } else {
            this.adjutant.execute(() -> doAddErrorInEventLoop(error));
        }
    }

    final void addError(ResultType resultType) {
        if (this.adjutant.inEventLoop()) {
            doAddErrorInEventLoop(resultType);
        } else {
            this.adjutant.execute(() -> doAddErrorInEventLoop(resultType));
        }
    }


    final void doAddErrorInEventLoop(Throwable error) {
        List<Throwable> errorList = this.errorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
            this.errorList = errorList;
        }
        errorList.add(error);
    }


    private void doAddErrorInEventLoop(ResultType resultType) {
        List<Throwable> errorList = this.errorList;
        if (errorList == null || errorList.isEmpty()) {
            doAddErrorInEventLoop(new SubscribeException(getSubscribeType(), resultType));
            return;
        }
        boolean add = true;
        switch (resultType) {
            case UPDATE:
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
            doAddErrorInEventLoop(new SubscribeException(getSubscribeType(), resultType));
        }

    }


}
