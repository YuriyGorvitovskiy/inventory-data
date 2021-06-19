package org.statemach.db.sql.postgres;

import org.statemach.db.jdbc.Extract;

import io.vavr.Tuple2;

public interface PostgresExtract {

    static final Extract<String> STRING = (rs, i) -> new Tuple2<>(rs.getString(i), i + 1);

}
