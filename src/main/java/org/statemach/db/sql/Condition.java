package org.statemach.db.sql;

import org.statemach.db.jdbc.Inject;

public class Condition extends Fragment {

    public static final Condition NONE = new Condition(SQL.TRIVIAL, Inject.NOTHING);

    public Condition(String sql, Inject inject) {
        super(sql, inject);
    }

}
