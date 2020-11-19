package io.jdbd.vendor;


import io.jdbd.lang.Nullable;

import java.nio.file.Path;

public interface StatementTask<T> {

    @Nullable
    T start();

    boolean decode(T cumulateBuf);

    @Nullable
    T moreSendPacket();

    @Nullable
    TaskPhase getTaskPhase();

    @Nullable
    default Path moreSendFile() {
        return null;
    }


    enum TaskPhase {
        SUBMITTED,
        STARTED,
        END
    }
}
