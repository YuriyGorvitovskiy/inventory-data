package com.yg.inventory.query.db;

import com.yg.util.Java;

import io.vavr.collection.List;

/**
 * - Conditions
 *      Straight: IS NULL,     EQUAL,     IN,     LESSER,     GREATER,     BETWEEN,     LIKE,     TEXT_SEARCH
 *       Inverse: IS NOT NULL, NOT EQUAL, NOT IN, NOT LESSER, NOT GREATER, NOT BETWEEN, NOT LIKE, NOT_IN_TEXT
 *
 * - Condition Values:
 *      IS NULL:                  1 - Path, COALESCE(...Paths), GREATEST(...Paths), LEAST(...Paths)
 *
 *      EQUAL, LESSER, GREATER:   1 - Path, COALESCE(...Paths), GREATEST(...Paths), LEAST(...Paths)
 *                                2 - Path, COALESCE(...Paths), GREATEST(...Paths), LEAST(...Paths), Literal
 *
 *      BETWEEN:                  1 - Path, COALESCE(...Paths), GREATEST(...Paths), LEAST(...Paths)
 *                                2 - Literal
 *                                3 - Literal
 *
 *      IN:                       1 - Path, COALESCE(...Paths), GREATEST(...Paths), LEAST(...Paths)
 *                                2 - List of literal
 *
 *      LIKE, TEXT_SEARCH         1 - Path, COALESCE(...Paths)
 *                                2 - String Literal
 *
 * - Logical Grouping: AND, OR
 *      AND: List of Conditions & ORs
 *      OR:  List of Conditions & ANDs
 *
 * - Set Grouping: UNION, EXCEPT
 *      EXCEPT:     1 - UNION, AND, OR, Condition
 *                  2 - UNION, AND, OR, Condition
 *      UNION:      1 - List of UNIONs & ANDs & ORs & Conditions
 *
 *
 * @author yuriy
 *
 */
public class Comparison {
    public static enum Op {
        IS_NULL,
        EQUAL,
        IN,
        IN_WITH_NULL,
        LESSER,
        GREATER,
        BETWEEN,
        LIKE,
        TEXT_SEARCH;

        public boolean in(Op... checks) {
            return Java.in(this, checks);
        }
    }

    public final List<String> path;
    public final boolean      inverse;
    public final Op           op;
    public final List<Object> value;

    public Comparison(List<String> path,
                      boolean inverse,
                      Op op,
                      List<Object> value) {
        this.path = path;
        this.inverse = inverse;
        this.op = op;
        this.value = value;
    }

}
