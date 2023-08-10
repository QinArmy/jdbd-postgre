package io.jdbd.result;

import io.jdbd.session.Option;
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
     * <p>
     * The implementation of this method perhaps support some of following :
     *     <ul>
     *         <li>{@link Option#WARNING_COUNT}</li>
     *         <li>{@link Option#MESSAGE}</li>
     *         <li>{@link Option#VENDOR_CODE}</li>
     *     </ul>
     * </p>
     */
    @Override
    <T> T valueOf(Option<T> option);

    /**
     * @return message that contain warning.
     */
    String message();

}
