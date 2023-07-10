package io.jdbd.result;


/**
 * <p>
 * This representing row reader that read row from database client protocol.
 * </p>
 *
 * @see ResultRow
 * @since 1.0
 */
public interface CurrentRow extends JdbdRow {

    ResultRow asResultRow();


}
