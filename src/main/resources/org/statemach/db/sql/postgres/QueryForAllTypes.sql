SELECT t.typname
     , a.attname
     , pg_catalog.format_type(a.atttypid, null) AS data_type
     , information_schema._pg_char_max_length (a.atttypid, a.atttypmod) AS character_max_length
  FROM       pg_catalog.pg_type t
  INNER JOIN pg_catalog.pg_attribute a ON a.attrelid = t.typrelid
  INNER JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
  LEFT  JOIN information_schema.tables tb ON tb.table_name = t.typname AND tb.table_schema = n.nspname
  WHERE n.nspname  = ? AND tb.table_name IS NULL
  ORDER BY t.typname ASC, a.attnum ASC