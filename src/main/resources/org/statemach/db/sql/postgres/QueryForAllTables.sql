SELECT table_name, column_name, data_type
    FROM  information_schema.columns c
    WHERE table_schema = 'public'
    ORDER BY ordinal_position ASC
