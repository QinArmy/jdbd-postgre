package io.jdbd.meta;

import io.jdbd.session.OptionSpec;

public interface IndexColumnMeta extends OptionSpec {

    String columnName();

    Sorting sorting();


}
