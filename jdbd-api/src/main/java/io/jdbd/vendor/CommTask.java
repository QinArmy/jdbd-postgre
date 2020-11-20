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

    enum TaskPhase {
        SUBMITTED,
        STARTED,
        END
    }
}
