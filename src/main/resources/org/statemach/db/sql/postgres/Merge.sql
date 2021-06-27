INSERT INTO ${0}.${1} (${2})
    VALUES (${3})
    ON CONFLICT (${4}) DO UPDATE SET ${5}
    ${6}
