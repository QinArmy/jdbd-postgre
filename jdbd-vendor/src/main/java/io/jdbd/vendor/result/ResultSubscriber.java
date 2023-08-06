package io.jdbd.vendor.result;

import io.jdbd.result.ResultItem;
import org.reactivestreams.Subscriber;

interface ResultSubscriber extends Subscriber<ResultItem> {

    boolean isCancelled();
}
