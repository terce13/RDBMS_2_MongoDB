package com.mongodb.rterceno;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoTimeoutException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ruben on 23/01/2017.
 */
public class RDBMS_2_MongoDB {

    static Connection conn = null;
    static DatabaseMetaData md;
    static MongoClient mongoClient;

    private static void showUsage(){
        System.out.println("Incorrect number of parameters. Use: ");
        System.out.println("java -jar RDBMS_2_MongoDB.jar <database name> <RDBMS url> [<mongodb URI>] [<Transform (true/false)>]");
        System.out.println("Example: java -jar RDBMS_2_MongoDB.jar Chinook \"mysql://localhost:3306?user=myUser&password=myPassword\" mongodb://myUser:myPassword@localhost:27017 true");

    }

    public static void main(String[] args) {

        if (args.length < 2 || args.length > 4 || args[0].equals("help")){
            showUsage();
            System.exit(0);
        }

        ResultSet rs = null;

        try {
            //Connecting to MySQL

            // The newInstance() call is a work around for some broken Java implementations
            Class.forName("com.mysql.jdbc.Driver").newInstance();

            conn = DriverManager.getConnection("jdbc:" + args[1]);

            conn.setCatalog(args[0]);

            //Connecting to MongoDB

            //This allows quick failure if the connection to MongoDB doesn't work.
            MongoClientOptions.Builder options = new MongoClientOptions.Builder().serverSelectionTimeout(5000);
            //options.writeConcern(new WriteConcern(2));

            if (args.length == 3) {
                mongoClient = new MongoClient(new MongoClientURI(args[2], options));
            } else {
                // defaults to localhost:27017
                mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017", options));
            }

            //Checking if MongoClient has connected succesfully
            mongoClient.listDatabaseNames().first();

            //Getting Table information
            md = conn.getMetaData();

            //Only user tables, not views, system tables or others.
            String[] types = {"TABLE"};

            rs = md.getTables(args[0], null, "%", types);

            String tableName;
            List<Thread> threads = new ArrayList<Thread>(rs.getFetchSize());

            while (rs.next()) {
                tableName = rs.getString("TABLE_NAME");
                Runnable r = new Load_Table(tableName, args[0]);
                Thread t = new Thread(r);
                t.setName(tableName);
                threads.add(t);
                t.start();
            }

            for (int i = 0; i < threads.size(); i++) {
                threads.get(i).join();
            }
            conn.close();

            //Creating collections in MongoDB
            if (args[3].equals("true")){
                Transformation.transformData(args[0]);
            }


        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());

        } catch (MongoTimeoutException e) {
            System.out.printf("Failed to connect to MongoDB, please, check connection string\n");
            e.printStackTrace();

        } catch (IllegalAccessException e) {
            e.printStackTrace();

        } catch (InterruptedException e) {
            e.printStackTrace();

        } catch (InstantiationException e) {
            e.printStackTrace();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();

        } finally {

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                } // ignore

                rs = null;
            }
            mongoClient.close();
        }

    }
}
