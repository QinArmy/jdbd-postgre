package io.jdbd.postgre.protocol.client;

import io.jdbd.result.ServerException;
import io.jdbd.session.Option;

import java.util.Map;

/**
 * <p>
 * Emit, when postgre server return error message.
 * </p>
 *
 * @since 1.0
 */
public final class PgServerException extends ServerException implements PgErrorOrNotice {


    private final Map<Byte, String> fieldMap;

    /**
     * package constructor
     */
    PgServerException(Map<Byte, String> fieldMap) {
        super(fieldMap.getOrDefault(ErrorMessage.MESSAGE, ""), fieldMap.get(ErrorMessage.SQLSTATE), 0);
        this.fieldMap = fieldMap;
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T> T valueOf(final Option<T> option) {
        final Byte code;
        code = ErrorMessage.OPTION_MAP.get(option);
        if (code == null) {
            return null;
        }
        return (T) this.fieldMap.get(code);
    }


}
