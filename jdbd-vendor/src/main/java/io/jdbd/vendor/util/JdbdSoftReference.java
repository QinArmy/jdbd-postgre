package io.jdbd.vendor.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

public final class JdbdSoftReference<T> extends SoftReference<T> {

    public static <T> JdbdSoftReference<T> reference(T referent) {
        return new JdbdSoftReference<>(referent);
    }

    public static <T> JdbdSoftReference<T> reference(T referent, ReferenceQueue<? super T> q) {
        return new JdbdSoftReference<>(referent, q);
    }

    private JdbdSoftReference(T referent) {
        super(referent);
    }

    private JdbdSoftReference(T referent, ReferenceQueue<? super T> q) {
        super(referent, q);
    }


}
