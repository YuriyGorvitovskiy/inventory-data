package org.statemach.db.sql;

import org.statemach.db.jdbc.Inject;

public class Fragment {

    public final String sql;
    public final Inject inject;

    public Fragment(String sql, Inject inject) {
        this.sql = sql;
        this.inject = inject;
    }

}
