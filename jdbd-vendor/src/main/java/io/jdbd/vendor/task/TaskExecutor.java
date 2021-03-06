package io.jdbd.vendor.task;

public interface TaskExecutor<T extends TaskAdjutant> {

    T getAdjutant();

}
