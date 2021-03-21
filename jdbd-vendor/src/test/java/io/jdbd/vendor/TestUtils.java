package io.jdbd.vendor;

import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class TestUtils {


    public static Path getResourcesPath() {
        return Paths.get(getModulePath().toAbsolutePath().toString(), "src/test/resources");
    }

    public static Path getModulePath() {
        Path userDir = Paths.get(System.getProperty("user.dir"));
        Path modulePath;
        if (userDir.endsWith("jdbd-vendor")) {
            modulePath = userDir;
        } else {
            modulePath = Paths.get(userDir.toAbsolutePath().toString(), "jdbd-vendor");
        }
        return modulePath;
    }

    public static Path getTargetTestClassesPath() {
        return Paths.get(getModulePath().toAbsolutePath().toString(), "target/test-classes");
    }


}
