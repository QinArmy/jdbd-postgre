package io.jdbd.vendor.stmt;

import io.jdbd.stmt.MultiStatement;
import io.jdbd.vendor.result.ReactorMultiResult;

public interface ReactorMultiStatement extends MultiStatement {

    @Override
    ReactorMultiResult executeAsMulti();
}
