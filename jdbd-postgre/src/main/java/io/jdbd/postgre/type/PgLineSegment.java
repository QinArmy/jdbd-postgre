package io.jdbd.postgre.type;

import io.jdbd.type.LongBinary;

import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LSEG">Lines</a>
 */
final class PgLineSegment implements LongBinary, PrintableData {

    static PgLineSegment parse(final String textValue, final boolean bigEndian)
            throws IllegalArgumentException {
        return new PgLineSegment(textValue, PgTypes.lineSegmentToWkb(textValue, bigEndian));
    }

    private final String textValue;

    private final byte[] wkbBytes;

    private PgLineSegment(String textValue, byte[] wkbBytes) {
        this.textValue = textValue;
        this.wkbBytes = wkbBytes;
    }

    @Override
    public final boolean isArray() {
        return true;
    }

    @Override
    public final byte[] asArray() throws IllegalStateException {
        return Arrays.copyOf(this.wkbBytes, this.wkbBytes.length);
    }

    @Override
    public final FileChannel openReadOnlyChannel() throws IllegalStateException {
        throw new IllegalStateException("");
    }

    @Override
    public final String toString() {
        return this.textValue;
    }


}
