package io.jdbd.vendor.task;

public interface TaskExecutor<T extends ITaskAdjutant> {

    /**
     * @return always same instance.
     */
    T getAdjutant();

}
