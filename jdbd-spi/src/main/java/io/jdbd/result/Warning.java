package io.jdbd.result;

import io.jdbd.session.OptionSpec;

/**
 * <p>
 * This interface representing database server response warning.
 * </p>
 *
 * @see io.jdbd.session.Option#WARNING_COUNT
 * @since 1.0
 */
public interface Warning extends OptionSpec {


    /**
     * @return message that contain warning.
     */
    String warningMessage();

}
