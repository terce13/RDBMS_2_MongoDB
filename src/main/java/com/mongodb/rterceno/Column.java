package com.mongodb.rterceno;

/**
 * Created by Ruben on 23/01/2017.
 */
public class Column {
    private String name;
    private String type;

    public Column(String columnName, String columnType){
        name = columnName;
        type = columnType;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}
