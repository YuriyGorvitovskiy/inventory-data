SELECT column_name, data_type
    FROM  information_schema.columns c
    WHERE table_schema = 'public' AND table_name= ?
    ORDER BY ordinal_position ASC
