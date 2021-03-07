package io.jdbd.vendor.task;

public interface TaskExecutor<T extends TaskAdjutant> {

    /**
     * @return always same instance.
     */
    T getAdjutant();

}
