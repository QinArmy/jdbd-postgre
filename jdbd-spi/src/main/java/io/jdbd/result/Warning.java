package io.jdbd.result;

import io.jdbd.session.OptionSpec;

public interface Warning extends OptionSpec {

    /**
     * @return message that contain warning.
     */
    String getMessage();

}
