package io.jdbd.vendor.type;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Deprecated
abstract class TempFiles {

    private TempFiles() {
    }

    private static final ConcurrentMap<Path, Boolean> PATH_MAP = new ConcurrentHashMap<>();


    static void addTempPath(Path path) {
        PATH_MAP.putIfAbsent(path, Boolean.TRUE);
    }

    static Boolean removeTempPath(Path path) {
        return PATH_MAP.remove(path);
    }

    private static void deleteTempPath() {
        for (Path path : PATH_MAP.keySet()) {
            try {
                Files.deleteIfExists(path);
            } catch (Throwable e) {
                //here don't need throw exception
            }
        }
        PATH_MAP.clear();
    }


    static {
        Runtime.getRuntime().addShutdownHook(new Thread(TempFiles::deleteTempPath));
    }


}
