package org.statemach.db.sql;

import org.statemach.db.jdbc.Inject;
import org.statemach.db.schema.Schema;
import org.statemach.db.schema.TableInfo;

public class TableLike extends Fragment {

    public TableLike(String sql, Inject inject) {
        super(sql, inject);
    }

    public static TableLike of(Schema schema, TableInfo table) {
        return new TableLike(schema.name + SQL.DOT + table.name, Inject.NOTHING);
    }

    public static TableLike of(View<?> view) {
        return new TableLike(view.name, Inject.NOTHING);
    }

    public static TableLike of(String selectSql, Inject inject) {
        return new TableLike(selectSql, inject);
    }
}
