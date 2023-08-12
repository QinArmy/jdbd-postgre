package io.jdbd.postgre.protocol.client;


import io.jdbd.meta.DataType;
import io.jdbd.result.CurrentRow;

/**
 * <p>
 * This class representing postgre internal data type. For example regclass,xid,cid,xid8 .
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/datatype-oid.html">Object Identifier Types</a>
 * @see <a href="https://www.postgresql.org/docs/current/catalog-pg-type.html">pg_type</a>
 * @since 1.0
 */
final class InternalType implements DataType {

    final int oid;

    final String typeName;

    final TypeCategory category;

    private InternalType(CurrentRow row) {
        this.oid = row.getNonNull(0, Integer.class);
        this.typeName = row.getNonNull(1, String.class);
        this.category = TypeCategory.from(row.getNonNull(3, String.class));
    }

    @Override
    public String name() {
        return this.typeName();
    }

    @Override
    public String typeName() {
        return this.typeName;
    }

    @Override
    public boolean isArray() {
        return this.category == TypeCategory.ARRAY;
    }

    @Override
    public boolean isUnknown() {
        return false;
    }

    @Override
    public boolean isUserDefined() {
        return false;
    }

    @Override
    public String toString() {
        return super.toString();
    }


}
