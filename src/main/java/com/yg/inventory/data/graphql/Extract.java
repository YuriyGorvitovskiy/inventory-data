package com.yg.inventory.data.graphql;

import com.yg.util.DB.DataType;

import io.vavr.collection.List;

public class Extract {
    static final String NAME_DELIMITER = ".";

    final String       name;
    final List<String> path;
    final DataType     type;

    Extract(String name, List<String> path, DataType type) {
        this.name = name;
        this.path = path;
        this.type = type;
    }

    public static Extract of(List<String> path, DataType type) {
        return new Extract(path.mkString(NAME_DELIMITER), path, type);
    }
}
