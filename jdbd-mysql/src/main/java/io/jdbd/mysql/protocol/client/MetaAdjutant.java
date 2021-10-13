package io.jdbd.mysql.protocol.client;

public interface MetaAdjutant {

    TaskAdjutant adjutant();

    void updateSequenceId(int sequenceId);
}
