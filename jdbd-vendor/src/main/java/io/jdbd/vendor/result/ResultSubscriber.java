package io.jdbd.vendor.result;

import io.jdbd.result.Result;
import org.reactivestreams.Subscriber;

interface ResultSubscriber extends Subscriber<Result> {

    boolean isCancelled();
}
