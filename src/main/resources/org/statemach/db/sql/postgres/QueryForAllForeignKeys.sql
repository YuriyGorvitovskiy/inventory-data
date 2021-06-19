SELECT rc.constraint_name
     , scu.table_name
     , fcu.table_name
     , scu.column_name
     , fcu.column_name
  FROM information_schema.referential_constraints rc
  JOIN information_schema.key_column_usage        scu  ON  scu.constraint_name = rc.constraint_name
  JOIN information_schema.key_column_usage        fcu  ON  fcu.constraint_name = rc.unique_constraint_name
                                                       AND fcu.ordinal_position = scu.position_in_unique_constraint
                                                       AND fcu.table_schema = scu.table_schema
 WHERE scu.table_schema = ?
 ORDER BY scu.ordinal_position