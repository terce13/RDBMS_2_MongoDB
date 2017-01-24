package com.mongodb.rterceno;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;

import static com.mongodb.client.model.Aggregates.addFields;
import static com.mongodb.client.model.Aggregates.lookup;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.out;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.rterceno.RDBMS_2_MongoDB.mongoClient;

/**
 * Created by Ruben on 24/01/2017.
 */
public class Transformation {
    static MongoDatabase db = null;

    public static void transformData(String databaseName) {
        db = mongoClient.getDatabase(databaseName);
        createTrack();//Collection
        createPlaylist();//Collection
        //createInvoces();//Collection
        //createCustomer();//View
    }

    private static void createCustomer() {

    }

    private static void createInvoces() {

    }

    private static void createPlaylist() {
        MongoCollection<Document> coll = db.getCollection("PlaylistTrack");

        Bson tracksLookup = lookup("tracks", "TrackId", "TrackId", "Tracks");
        Bson project1 = project(fields(
                include("PlaylistId"),
                computed("Track.Name", new Document("$arrayElemAt", Arrays.asList("$Tracks.Name", 0))),
                computed("Track.Album", new Document("$arrayElemAt", Arrays.asList("$Tracks.Album", 0))),
                computed("Track.Milliseconds", new Document("$arrayElemAt", Arrays.asList("$Tracks.Milliseconds", 0))),
                computed("Track.Bytes", new Document("$arrayElemAt", Arrays.asList("$Tracks.Bytes", 0))),
                computed("Track.Genre", new Document("$arrayElemAt", Arrays.asList("$Tracks.Genre", 0)))
        ));
        Bson group = group("$PlaylistId", Accumulators.push("Tracks", "$Track"));
        Bson PlaylistLookup = lookup("Playlist", "_id", "PlaylistId", "Playlist");
        Bson project2 = project(fields(
                exclude("_id"),
                include("Tracks"),
                computed("PlaylistId", "$_id"),
                computed("Name",new Document("$arrayElemAt", Arrays.asList("$Playlist.Name", 0)))
        ));
        Bson out = out("playlists");

        coll.aggregate(Arrays.asList(tracksLookup, project1, group, PlaylistLookup, project2, out)).toCollection();

    }

    private static void createTrack() {
        MongoCollection<Document> coll = db.getCollection("Track");

        Bson MediaTypeLookup = lookup("MediaType", "MediaTypeId", "MediaTypeId", "MediaType");
        Bson GenreLookup = lookup("Genre", "GenreId", "GenreId", "Genre");
        Bson AlbumLookup = lookup("Album", "AlbumId", "AlbumId", "Album");
        Bson ArtistLookup = lookup("Artist", "Album.ArtistId", "ArtistId", "Album.Artist");
        Bson addFields1 = addFields(
                new Field("Album", new Document("$arrayElemAt", Arrays.asList("$Album", 0))),
                new Field("MediaType", new Document("$arrayElemAt", Arrays.asList("$MediaType.Name", 0))),
                new Field("Genre", new Document("$arrayElemAt", Arrays.asList("$Genre.Name", 0)))
                );
        Bson project = project(fields(
                computed("Album.Artist", new Document("$arrayElemAt", Arrays.asList("$Album.Artist.Name", 0))),
                include(
                "TrackId",
                "Name",
                "MediaType",
                "Genre",
                "Composer",
                "Album.Title",
                "Album.Artist",
                "Milliseconds",
                "Bytes",
                "UnitPrice"
            )));
        

        Bson out = out("tracks");

        coll.aggregate(Arrays.asList(MediaTypeLookup, GenreLookup, AlbumLookup, addFields1, ArtistLookup, project, out)).toCollection();
        coll = db.getCollection("tracks");
        coll.createIndex(new Document("TrackId", 1), new IndexOptions().unique(true));
    }
}
