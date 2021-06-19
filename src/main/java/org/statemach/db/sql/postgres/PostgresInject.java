package org.statemach.db.sql.postgres;

import java.util.function.Function;

import org.statemach.db.jdbc.Inject;

public interface PostgresInject {

    static final Function<String, Inject> STRING = (v) -> (ps, i) -> {
        ps.setString(i, v);
        return i + 1;
    };

}
