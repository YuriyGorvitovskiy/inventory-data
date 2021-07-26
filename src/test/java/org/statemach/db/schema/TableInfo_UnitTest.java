package org.statemach.db.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;

public class TableInfo_UnitTest {

    final String TABLE_NAME_1 = "Table1";
    final String TABLE_NAME_2 = "Table2";

    final String COLUMN_NAME_1 = "Column1";
    final String COLUMN_NAME_2 = "Column2";
    final String COLUMN_NAME_3 = "Column3";

    final DataType DATA_TYPE_1 = new DataType("DataType1");
    final DataType DATA_TYPE_2 = new DataType("DataType2");
    final DataType DATA_TYPE_3 = new DataType("DataType3");

    final ColumnInfo COLUMN_1 = new ColumnInfo(COLUMN_NAME_1, DATA_TYPE_1, null);
    final ColumnInfo COLUMN_2 = new ColumnInfo(COLUMN_NAME_2, DATA_TYPE_2, null);
    final ColumnInfo COLUMN_3 = new ColumnInfo(COLUMN_NAME_3, DATA_TYPE_3, null);

    final Map<String, ColumnInfo> COLUMNS_1 = List.of(COLUMN_1, COLUMN_2).toLinkedMap(c -> c.name, c -> c);
    final Map<String, ColumnInfo> COLUMNS_2 = List.of(COLUMN_2, COLUMN_1).toLinkedMap(c -> c.name, c -> c);
    final Map<String, ColumnInfo> COLUMNS_3 = List.of(COLUMN_1, COLUMN_3).toLinkedMap(c -> c.name, c -> c);
    final Map<String, ColumnInfo> COLUMNS_4 = List.of(COLUMN_1).toLinkedMap(c -> c.name, c -> c);

    final Option<PrimaryKey> PRIMARY_KEY_1 = Option.of(new PrimaryKey("PrimaryKey1", TABLE_NAME_1, List.empty()));
    final Option<PrimaryKey> PRIMARY_KEY_2 = Option.of(new PrimaryKey("PrimaryKey2", TABLE_NAME_2, List.empty()));

    final ForeignKey FOREIGN_KEY_1 = new ForeignKey("ForeignKey1", TABLE_NAME_1, TABLE_NAME_2, List.empty());
    final ForeignKey FOREIGN_KEY_2 = new ForeignKey("ForeignKey2", TABLE_NAME_1, TABLE_NAME_2, List.empty());
    final ForeignKey FOREIGN_KEY_3 = new ForeignKey("ForeignKey3", TABLE_NAME_1, TABLE_NAME_2, List.empty());
    final ForeignKey FOREIGN_KEY_4 = new ForeignKey("ForeignKey4", TABLE_NAME_2, TABLE_NAME_1, List.empty());
    final ForeignKey FOREIGN_KEY_5 = new ForeignKey("ForeignKey5", TABLE_NAME_2, TABLE_NAME_1, List.empty());
    final ForeignKey FOREIGN_KEY_6 = new ForeignKey("ForeignKey6", TABLE_NAME_2, TABLE_NAME_1, List.empty());

    final Map<String, ForeignKey> OUTGOING_1 = List.of(FOREIGN_KEY_1, FOREIGN_KEY_2).toLinkedMap(f -> f.name, f -> f);
    final Map<String, ForeignKey> OUTGOING_2 = List.of(FOREIGN_KEY_2, FOREIGN_KEY_1).toLinkedMap(f -> f.name, f -> f);
    final Map<String, ForeignKey> OUTGOING_3 = List.of(FOREIGN_KEY_1, FOREIGN_KEY_3).toLinkedMap(f -> f.name, f -> f);
    final Map<String, ForeignKey> OUTGOING_4 = List.of(FOREIGN_KEY_1).toLinkedMap(f -> f.name, f -> f);

    final Map<String, ForeignKey> INCOMING_1 = List.of(FOREIGN_KEY_4, FOREIGN_KEY_5).toLinkedMap(f -> f.name, f -> f);
    final Map<String, ForeignKey> INCOMING_2 = List.of(FOREIGN_KEY_5, FOREIGN_KEY_4).toLinkedMap(f -> f.name, f -> f);
    final Map<String, ForeignKey> INCOMING_3 = List.of(FOREIGN_KEY_4, FOREIGN_KEY_6).toLinkedMap(f -> f.name, f -> f);
    final Map<String, ForeignKey> INCOMING_4 = List.of(FOREIGN_KEY_4).toLinkedMap(f -> f.name, f -> f);

    final TableInfo subject = new TableInfo(TABLE_NAME_1, COLUMNS_1, PRIMARY_KEY_1, INCOMING_1, OUTGOING_1);
    final TableInfo other1  = new TableInfo(TABLE_NAME_1, COLUMNS_1, PRIMARY_KEY_1, INCOMING_1, OUTGOING_1);
    final TableInfo other2  = new TableInfo(TABLE_NAME_2, COLUMNS_1, PRIMARY_KEY_1, INCOMING_1, OUTGOING_1);
    final TableInfo other3  = new TableInfo(TABLE_NAME_1, COLUMNS_2, PRIMARY_KEY_1, INCOMING_1, OUTGOING_1);
    final TableInfo other4  = new TableInfo(TABLE_NAME_1, COLUMNS_3, PRIMARY_KEY_1, INCOMING_1, OUTGOING_1);
    final TableInfo other5  = new TableInfo(TABLE_NAME_1, COLUMNS_4, PRIMARY_KEY_1, INCOMING_1, OUTGOING_1);
    final TableInfo other6  = new TableInfo(TABLE_NAME_1, COLUMNS_1, PRIMARY_KEY_2, INCOMING_1, OUTGOING_1);
    final TableInfo other7  = new TableInfo(TABLE_NAME_1, COLUMNS_1, PRIMARY_KEY_1, INCOMING_2, OUTGOING_1);
    final TableInfo other8  = new TableInfo(TABLE_NAME_1, COLUMNS_1, PRIMARY_KEY_1, INCOMING_3, OUTGOING_1);
    final TableInfo other9  = new TableInfo(TABLE_NAME_1, COLUMNS_1, PRIMARY_KEY_1, INCOMING_4, OUTGOING_1);
    final TableInfo other10 = new TableInfo(TABLE_NAME_1, COLUMNS_1, PRIMARY_KEY_1, INCOMING_1, OUTGOING_2);
    final TableInfo other11 = new TableInfo(TABLE_NAME_1, COLUMNS_1, PRIMARY_KEY_1, INCOMING_1, OUTGOING_3);
    final TableInfo other12 = new TableInfo(TABLE_NAME_1, COLUMNS_1, PRIMARY_KEY_1, INCOMING_1, OUTGOING_4);

    @Test
    void hashCode_test() {
        // Execute
        int result  = subject.hashCode();
        int result1 = other1.hashCode();
        int result2 = other3.hashCode();
        int result3 = other7.hashCode();
        int result4 = other10.hashCode();

        // Verify
        assertEquals(result, result1);
        assertEquals(result, result2);
        assertEquals(result, result3);
        assertEquals(result, result4);
    }

    @Test
    void equals_test() {
        // Execute
        boolean result   = subject.equals(subject);
        boolean result1  = subject.equals(other1);
        boolean result2  = subject.equals(other2);
        boolean result3  = subject.equals(other3);
        boolean result4  = subject.equals(other4);
        boolean result5  = subject.equals(other5);
        boolean result6  = subject.equals(other6);
        boolean result7  = subject.equals(other7);
        boolean result8  = subject.equals(other8);
        boolean result9  = subject.equals(other9);
        boolean result10 = subject.equals(other10);
        boolean result11 = subject.equals(other11);
        boolean result12 = subject.equals(other12);

        // Verify
        assertTrue(result);
        assertTrue(result1);
        assertFalse(result2);
        assertTrue(result3);
        assertFalse(result4);
        assertFalse(result5);
        assertFalse(result6);
        assertTrue(result7);
        assertFalse(result8);
        assertFalse(result9);
        assertTrue(result10);
        assertFalse(result11);
        assertFalse(result12);
    }

    @Test
    void toString_test() {
        // Execute
        String result = subject.toString();

        // Verify
        assertTrue(result.contains(TABLE_NAME_1));
    }
}
