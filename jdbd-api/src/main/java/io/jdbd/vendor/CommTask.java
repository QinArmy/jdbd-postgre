package io.jdbd.vendor;

import io.jdbd.lang.Nullable;

import java.nio.file.Path;

public interface CommTask<T> {

    @Nullable
    T start();

    @Nullable
    TaskPhase getTaskPhase();

    boolean decode(T cumulateBuffer);

    @Nullable
    Path moreSendFile();

    @Nullable
    T moreSendPacket();

    void error(Throwable e);

    void onChannelClose();

    enum TaskPhase {
        SUBMITTED,
        STARTED,
        END
    }
}
