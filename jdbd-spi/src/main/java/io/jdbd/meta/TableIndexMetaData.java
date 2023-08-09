package io.jdbd.meta;

import io.jdbd.lang.Nullable;
import io.jdbd.session.OptionSpec;

import java.util.List;

public interface TableIndexMetaData extends OptionSpec {

    DatabaseTableMetaData tableMeta();

    String indexName();

    String indexType();

    KeyMode keyMode();

    BooleanMode visible();

    List<IndexColumnMeta> indexColumnList();

    @Nullable
    String comment();


}
