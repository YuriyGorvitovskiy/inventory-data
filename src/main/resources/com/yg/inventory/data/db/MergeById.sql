INSERT INTO ${1} (${2})
    VALUES (${3})
    ON CONFLICT DO UPDATE SET (${4})
        WHERE id = ?
    RETURNING ${5}
