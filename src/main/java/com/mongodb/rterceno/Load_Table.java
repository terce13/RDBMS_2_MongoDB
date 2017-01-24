package com.mongodb.rterceno;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.types.Decimal128;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.rterceno.RDBMS_2_MongoDB.conn;
import static com.mongodb.rterceno.RDBMS_2_MongoDB.md;
import static com.mongodb.rterceno.RDBMS_2_MongoDB.mongoClient;

/**
 * Created by Ruben on 23/01/2017.
 */
public class Load_Table implements Runnable {
    private String tableName;
    private String database;

    Load_Table(String table, String databaseName){
        tableName = table;
        database = databaseName;

    }

    public void run() {
        System.out.printf("Thread %s successfully invoked to process table %s.\n ", Thread.currentThread().getName(), tableName);

        Statement stmt;
        ResultSet rs = null;

        MongoDatabase mDB = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = mDB.getCollection(tableName);

        List<Column> columns = new ArrayList<Column>();
        try {

            rs = md.getColumns(database, null, tableName, "%");

            String name;
            String type;
            String columnType;

            while (rs.next()){
                name = rs.getString("COLUMN_NAME");
                type = rs.getString("TYPE_NAME");

                if (type.equals("INT")){
                    columnType = "Integer";
                }else if (type.equals("VARCHAR")){
                    columnType = "String";
                }else if(type.equals("DATETIME")) {
                    columnType = "ISODate";
                }else if(type.equals("DECIMAL")) {
                    columnType = "BigDecimal";
                }else {
                    System.out.printf("Found unrecognized column type in Table: %s, Column: %s, Type: %s\n", tableName, name, type);
                    //Default to String
                    columnType = "String";
                }
                columns.add(new Column(name, columnType));
            }

            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT * FROM " + tableName);

            List<WriteModel<Document>> writeActions = new ArrayList<WriteModel<Document>>();

            while (rs.next()){
                Document doc = new Document();
                for (int i = 0; i < columns.size(); i++){
                    name = columns.get(i).getName();
                    type = columns.get(i).getType();

                    switch (type) {
                        case "BigDecimal": {
                            // 3.4 version
                            doc.append(name, new Decimal128(rs.getBigDecimal(name)));

                            // 3.0/3.2 version
                            //doc.append(name, rs.getFloat(name));
                            break;
                        }
                        case "Integer": {
                            doc.append(name, Integer.valueOf(rs.getInt(name)));
                            break;
                        }
                        case "ISODate" : {
                            doc.append(name, rs.getDate(name));
                            break;
                        }
                        default: {
                            doc.append(name, rs.getString(name)); //We jump over the 4 firsts fields and all the empty fields on the CSV
                        }
                    }

                }
                writeActions.add(new InsertOneModel<>(doc));

                if (writeActions.size() == 100){
                    collection.bulkWrite(writeActions, new BulkWriteOptions().ordered(false));
                    writeActions.clear();
                }

            }

            if (writeActions.size() > 0){
                collection.bulkWrite(writeActions, new BulkWriteOptions().ordered(false));
            }

            //Creating unique index on the relational ID
            if (!tableName.equals("PlaylistTrack")) {
                collection.createIndex(new Document(tableName + "Id", 1), new IndexOptions().unique(true));
            }

            System.out.printf("Table %s successfully processed.\n", tableName);

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());

        }finally {
            // it is a good idea to release
            // resources in a finally{} block
            // in reverse-order of their creation
            // if they are no-longer needed

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) { } // ignore

                rs = null;
            }
        }
    }
}
