package io.jdbd.mysql.protocol.client;


import io.jdbd.result.CurrentRow;


final class MySQLCurrentRow extends MySQLRow.JdbdCurrentRow implements CurrentRow {


    private MySQLCurrentRow(MySQLRowMeta rowMeta) {
        super(rowMeta);
    }


}
