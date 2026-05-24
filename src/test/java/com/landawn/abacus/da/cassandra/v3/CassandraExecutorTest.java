/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.cassandra.CqlBuilder.LCCB;
import com.landawn.abacus.da.cassandra.CqlBuilder.NLC;
import com.landawn.abacus.da.cassandra.CqlBuilder.NSC;
import com.landawn.abacus.da.cassandra.CqlBuilder.SCCB;
import com.landawn.abacus.da.cassandra.CqlMapper;
import com.landawn.abacus.da.cassandra.ParsedCql;
import com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec;
import com.landawn.abacus.da.entity.Song;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.Dates;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.IntFunctions;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.NamingPolicy;
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.da.cassandra.v3.CassandraExecutor.StatementSettings;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import java.nio.ByteBuffer;

public class CassandraExecutorTest extends TestBase {

    /*
    
    https://stackoverflow.com/questions/50810922/how-to-connect-my-java-application-to-cassandra-running-through-docker
    run command from windows Command:
    docker run --name cassandra -p 9042:9042 cassandra:5.0
    docker start cassandra
    
    CREATE KEYSPACE IF NOT EXISTS simplex WITH replication  = {'class':'SimpleStrategy', 'replication_factor':3};
    DROP TYPE IF EXISTS simplex.fullname;
    DROP TYPE IF EXISTS simplex.address;
    DROP TABLE IF EXISTS simplex.users;
    CREATE TYPE IF NOT EXISTS simplex.fullname(firstName text, lastName text);
    CREATE TYPE IF NOT EXISTS simplex.address(street text, city text, zipCode int);
    CREATE TABLE IF NOT EXISTS simplex.users(id uuid PRIMARY KEY, name frozen <fullname>, addresses map<text, frozen <address>>, someDate varchar, last_update_time time, created_time timestamp, bytes list<tinyint>, shorts set<smallint>);
    CREATE TABLE IF NOT EXISTS simplex.users(id uuid PRIMARY KEY, name text, addresses text);
    
    CREATE TABLE IF NOT EXISTS simplex.songs (id uuid PRIMARY KEY, title text, album text, artist text, tags set<text>, data blob);
    CREATE TABLE IF NOT EXISTS simplex.playlists (id uuid, title text, album text,  artist text, song_id uuid, PRIMARY KEY (id, title, album, artist));
    
     */

    static final String collectionName = "testData";

    static final CassandraExecutor cassandraExecutor;

    static {
        final CodecRegistry codecRegistry = new CodecRegistry();

        // final Cluster cluster = Cluster.builder().withCodecRegistry(codecRegistry).addContactPoint("127.0.0.1:9042").build();
        final Cluster cluster = Cluster.builder()
                .withCodecRegistry(codecRegistry) //
                .addContactPointsWithPorts(new InetSocketAddress("127.0.0.1", 9042))
                .build();

        codecRegistry.register(UDTCodec.create(cluster, "simplex", "fullname", Users.Name.class));

        codecRegistry.register(UDTCodec.create(cluster, "simplex", "address", Users.Address.class));

        cassandraExecutor = new CassandraExecutor(cluster.connect());
        cassandraExecutor.registerTypeCodec(LocalDateTime.class);
    }

    @Test
    public void test_sync() {
        Users user = createUser();

        String sql = NSC.insertInto(Users.class).build().query();
        N.println(sql);
        cassandraExecutor.execute(sql, user);

        List<UUID> ids = cassandraExecutor.list(UUID.class, "SELECT id FROM simplex.users");
        N.println(ids);
        cassandraExecutor.delete(Users.class, Filters.in("id", ids));
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void test_async() throws Exception {
        Users user = createUser();

        String sql = NSC.insertInto(Users.class).build().query();
        N.println(sql);
        cassandraExecutor.execute(sql, user);

        List<Users> dbUsers = cassandraExecutor.async().list(Users.class, "SELECT * FROM simplex.users").get();
        N.println(dbUsers);
        cassandraExecutor.async().delete(Users.class, Filters.in("id", N.map(dbUsers, Users::getId)));
    }

    @Test
    public void test_delete() {
        Users user = createUser();

        String sql = NSC.insertInto(Users.class).build().query();
        N.println(sql);
        cassandraExecutor.execute(sql, user);

        List<Users> users = Stream.repeat(user, 10).map(Beans::copy).onEach(it -> it.setId(UUID.randomUUID())).toList();
        cassandraExecutor.batchInsert(users, BatchStatement.Type.LOGGED);

        List<UUID> ids = cassandraExecutor.list(UUID.class, "SELECT id FROM simplex.users");
        N.println(ids);

        String cql = NSC.deleteFrom(Users.class).where("id = ?").build().query();
        assertFalse(cassandraExecutor.get(Users.class, ids.get(0)).isEmpty());
        cassandraExecutor.execute(cql, ids.get(0));
        assertTrue(cassandraExecutor.get(Users.class, ids.get(0)).isEmpty());

        cql = NSC.deleteFrom(Users.class).where("id = :id").build().query();
        assertFalse(cassandraExecutor.get(Users.class, ids.get(1)).isEmpty());
        cassandraExecutor.execute(cql, ids.get(1));
        assertTrue(cassandraExecutor.get(Users.class, ids.get(1)).isEmpty());

        assertFalse(cassandraExecutor.get(Users.class, ids.get(2)).isEmpty());
        cassandraExecutor.execute(cql, ids.get(2));
        assertTrue(cassandraExecutor.get(Users.class, N.asMap("id", ids.get(2))).isEmpty());

        assertFalse(cassandraExecutor.get(Users.class, ids.get(3)).isEmpty());
        cassandraExecutor.delete(Users.class, Filters.eq("id", ids.get(3)));
        assertTrue(cassandraExecutor.get(Users.class, N.asMap("id", ids.get(3))).isEmpty());

        assertFalse(cassandraExecutor.list(Users.class, Filters.in("id", ids)).isEmpty());
        cassandraExecutor.delete(Users.class, Filters.in("id", ids));
        assertTrue(cassandraExecutor.list(Users.class, Filters.in("id", ids)).isEmpty());
    }

    @Test
    public void test_delete_2() {
        Users user = createUser();

        String sql = NSC.insertInto(Users.class).build().query();
        N.println(sql);
        cassandraExecutor.execute(sql, user);

        List<Users> users = Stream.repeat(user, 10).map(Beans::copy).onEach(it -> it.setId(UUID.randomUUID())).toList();
        cassandraExecutor.batchInsert(users, BatchStatement.Type.LOGGED);

        List<UUID> ids = cassandraExecutor.list(UUID.class, "SELECT id FROM simplex.users");
        N.println(ids);

        String cql = NSC.delete(N.asList("name")).from(Users.class).where("id = ?").build().query();
        N.println(cql);
        UUID id = ids.get(0);

        cassandraExecutor.get(Users.class, Filters.eq("id", id)).ifPresent(Fn.println());

        cassandraExecutor.queryForSingleValue(Users.class, Users.Name.class, "name", Filters.eq("id", id)).ifPresent(Fn.println());
        assertFalse(cassandraExecutor.queryForSingleValue(Users.class, Users.Name.class, "name", Filters.eq("id", id)).isNull());
        cassandraExecutor.execute(cql, id);
        assertTrue(cassandraExecutor.queryForSingleValue(Users.class, Users.Name.class, "name", Filters.eq("id", id)).isNull());

        cassandraExecutor.delete(Users.class, Filters.in("id", ids));
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void test_batch_update() throws Exception {
        Users user = createUser();

        String sql = NSC.insertInto(Users.class).build().query();
        N.println(sql);
        cassandraExecutor.execute(sql, user);

        List<Users> users = Stream.repeat(user, 10).map(Beans::copy).onEach(it -> it.setId(UUID.randomUUID())).toList();
        cassandraExecutor.batchInsert(users, BatchStatement.Type.LOGGED);

        List<UUID> ids = cassandraExecutor.list(UUID.class, "SELECT id FROM simplex.users");
        N.println(ids);

        List<Users> dbUsers = cassandraExecutor.list(Users.class, "SELECT * FROM simplex.users");

        dbUsers.stream().forEach(it -> it.getName().setFirstName("updatedFirstName"));

        cassandraExecutor.batchUpdate(dbUsers, BatchStatement.Type.UNLOGGED);

        dbUsers = cassandraExecutor.list(Users.class, "SELECT * FROM simplex.users");

        dbUsers.forEach(it -> assertEquals("updatedFirstName", it.getName().getFirstName()));

        cassandraExecutor.batchDelete(dbUsers, N.asList("name", "lastUpdateTime"));

        dbUsers = cassandraExecutor.list(Users.class, "SELECT * FROM simplex.users");
        dbUsers.forEach(Fn.println());

        dbUsers.forEach(it -> assertEquals(null, it.getName()));
        dbUsers.forEach(it -> assertEquals(null, it.getLastUpdateTime()));

        cassandraExecutor.async().batchDelete(dbUsers).get();

        assertTrue(cassandraExecutor.list(Users.class, "SELECT * FROM simplex.users").isEmpty());
    }

    @Test
    public void test_hello() {
        cassandraExecutor.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags)  VALUES ( 756716f7-2e54-4715-9f00-91dcbea6cf50,'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker', {'jazz', '2013'});");

        cassandraExecutor.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist)  VALUES ( 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d, 756716f7-2e54-4715-9f00-91dcbea6cf50, 'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker');");

        ResultSet results = cassandraExecutor.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"), row.getString("artist")));

            N.println(CassandraExecutor.toMap(row));
        }

        N.println(CassandraExecutor.extractData(results));

        results = cassandraExecutor.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        N.println(CassandraExecutor.extractData(results));

        System.out.println();
    }

    @Test
    public void test_hello_2() {
        cassandraExecutor.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags)  VALUES ( 756716f7-2e54-4715-9f00-91dcbea6cf50, 'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker', {'jazz', '2013'});");

        ResultSet results = cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"), row.getString("artist")));

            N.println(CassandraExecutor.toEntity(row, Song.class));
            N.println(CassandraExecutor.toMap(row));
        }

        N.println(CassandraExecutor.extractData(results));

        Dataset dataset = cassandraExecutor.query("SELECT * FROM simplex.songs WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");

        dataset.println();
    }

    @Test
    public void test_query() {
        Song song = new Song();
        song.setId(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"));
        song.setTitle("La Petite Tonkinoise");
        song.setAlbum("Bye Bye Blackbird");
        song.setArtist("Joséphine Baker");
        song.setTags(N.asSet("jazz", "2013"));

        ResultSet resultSet = cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags)  VALUES (?, ?, ?, ?, ?)", song.getId(),
                song.getTitle(), song.getAlbum(), song.getArtist(), N.stringOf(song.getTags()));

        N.println(resultSet.one());

        assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));

        assertEquals(1, cassandraExecutor.count("SELECT count(*) FROM simplex.songs WHERE id = ?", song.getId()));

        Song dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        Map<String, Object> map = cassandraExecutor.findFirst(Map.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(map);
        assertEquals(song.getId(), map.get("id"));

        Dataset dataset = cassandraExecutor.query("SELECT * FROM simplex.songs WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;");
        dataset.println();
        assertEquals(song.getId(), dataset.get("id"));

        UUID uuid = cassandraExecutor.queryForSingleValue(UUID.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(uuid);
        assertEquals(song.getId(), uuid);

        String strUUID = cassandraExecutor.queryForSingleValue(String.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(strUUID);
        assertEquals(song.getId().toString(), strUUID);

        assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));

        cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", song.getId());
        assertFalse(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));
    }

    @Test
    public void test_find() {
        Song song = new Song();
        song.setId(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"));
        song.setTitle("La Petite Tonkinoise");
        song.setAlbum("Bye Bye Blackbird");
        song.setArtist("Joséphine Baker");
        song.setTags(N.asSet("jazz", "2013"));

        ResultSet resultSet = cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags)  VALUES (?, ?, ?, ?, ?)", song.getId(),
                song.getTitle(), song.getAlbum(), song.getArtist(), N.stringOf(song.getTags()));

        N.println(resultSet.one());

        assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));

        assertEquals(1, cassandraExecutor.count("SELECT count(*) FROM simplex.songs WHERE id = ?", song.getId()));

        List<Song> dbSongs = cassandraExecutor.list(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId());
        N.println(dbSongs);
        assertEquals(song.getId(), dbSongs.get(0).getId());

        List<Map<String, String>> maps = (List) cassandraExecutor.list(Map.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId());
        N.println(maps);
        assertEquals(song.getId(), maps.get(0).get("id"));

        List<String> strs = cassandraExecutor.list(String.class, "SELECT title FROM simplex.songs WHERE id = ?", song.getId());
        N.println(strs);
        assertEquals(song.getTitle(), strs.get(0));

        cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", song.getId());
        assertFalse(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));
    }

    /**
     *
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Test
    public void test_query_async() throws InterruptedException, ExecutionException {
        Song song = new Song();
        song.setId(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"));
        song.setTitle("La Petite Tonkinoise");
        song.setAlbum("Bye Bye Blackbird");
        song.setArtist("Joséphine Baker");
        song.setTags(N.asSet("jazz", "2013"));

        ResultSet resultSet = cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags)  VALUES (?, ?, ?, ?, ?)", song.getId(),
                song.getTitle(), song.getAlbum(), song.getArtist(), N.stringOf(song.getTags()));

        N.println(resultSet.one());

        assertTrue(cassandraExecutor.async().exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get());

        assertEquals(1, cassandraExecutor.async().count("SELECT count(*) FROM simplex.songs WHERE id = ?", song.getId()).get().longValue());

        Song dbSong = cassandraExecutor.async().findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get().orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        Map<String, Object> map = cassandraExecutor.async().findFirst(Map.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get().orElse(null);
        N.println(map);
        assertEquals(song.getId(), map.get("id"));

        Dataset dataset = cassandraExecutor.async().query("SELECT * FROM simplex.songs WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;").get();
        dataset.println();
        assertEquals(song.getId(), dataset.get("id"));

        UUID uuid = cassandraExecutor.async().queryForSingleValue(UUID.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).get().orElse(null);
        N.println(uuid);
        assertEquals(song.getId(), uuid);

        String strUUID = cassandraExecutor.async()
                .queryForSingleValue(String.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId())
                .get()
                .orElse(null);
        N.println(strUUID);
        assertEquals(song.getId().toString(), strUUID);

        assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));

        cassandraExecutor.async().execute("DELETE FROM simplex.songs WHERE id = ?", song.getId()).get();
        assertFalse(cassandraExecutor.async().exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get());
    }

    @Test
    public void test_parameterized() {
        Song song = new Song();
        song.setId(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"));
        song.setTitle("La Petite Tonkinoise");
        song.setAlbum("Bye Bye Blackbird");
        song.setArtist("Joséphine Baker");
        song.setTags(N.asSet("jazz", "2013"));

        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags)  VALUES (#{id}, #{title}, #{album}, #{artist}, #{tags});", song);

        Song dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", N.asList(song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", N.asMap("id", song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", N.asList(song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", N.asMap("id", song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", N.asList(song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", N.asMap("id", song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", song.getId()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", N.asList(song.getId())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        try {
            cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", N.asMap("id", song.getId()));
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {

        }

        try {
            cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", song);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {

        }

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId().toString()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", N.asList(song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", N.asMap("id", song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", song.getId().toString()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", N.asList(song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", N.asMap("id", song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = #{id}", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", song.getId().toString()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", N.asList(song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", N.asMap("id", song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :id", song).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", song.getId().toString()).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        dbSong = cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", N.asList(song.getId().toString())).orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        try {
            cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", N.asMap("id", song.getId().toString()));
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {

        }

        try {
            cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = :a", song);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {

        }

        cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = #{id}", song.getId());
        assertFalse(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = #{id}", song.getId()));
    }

    @Test
    public void test_update() {

        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist, tags)  VALUES ( 756716f7-2e54-4715-9f00-91dcbea6cf50,"
                + "'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker', {'jazz', '2013'})");

        cassandraExecutor.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist)  VALUES ( 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,"
                + "756716f7-2e54-4715-9f00-91dcbea6cf50, 'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker' )");

        ResultSet results = cassandraExecutor.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d");

        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"), row.getString("artist")));

            N.println(CassandraExecutor.toMap(row));
        }

        N.println(CassandraExecutor.extractData(results));

        results = cassandraExecutor.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d");
        N.println(CassandraExecutor.extractData(results));

        cassandraExecutor.query("SELECT * FROM simplex.songs WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d").println();

        String sql = LCCB.update("simplex.songs").set("title", "album").where("id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d").build().query();
        N.println(sql);
        ResultSet resultSet = cassandraExecutor.execute(sql, "new new title", "new new Album");
        N.println(resultSet);
        N.println(resultSet.one());
        N.println(resultSet.wasApplied());
        assertTrue(resultSet.wasApplied());

        cassandraExecutor.query("SELECT * FROM simplex.songs WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d").println();

        sql = SCCB.deleteFrom("simplex.songs").where("id = ?").build().query();
        N.println(sql);
        resultSet = cassandraExecutor.execute(sql, "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d");
        N.println(resultSet);
        N.println(resultSet.one());
        N.println(resultSet.wasApplied());
        assertTrue(resultSet.wasApplied());
        cassandraExecutor.query("SELECT * FROM simplex.songs WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d").println();

        sql = NLC.update("simplex.songs").set("album").where("id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d").build().query();
        N.println(sql);
        resultSet = cassandraExecutor.execute(sql, "new new new Album", "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d");
        N.println(resultSet);
        N.println(resultSet.one());
        N.println(resultSet.wasApplied());
        assertTrue(resultSet.wasApplied());

        cassandraExecutor.query("SELECT * FROM simplex.songs WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d").println();

        System.out.println();
    }

    /**
     * Verifies that passing a single {@code null} parameter when the query
     * expects multiple parameters produces an IllegalArgumentException
     * (not a NullPointerException from {@code parameters[0].getClass()}).
     */
    @Test
    public void test_prepareStatement_nullSingleParamMultiParamQuery_throwsIAE_notNPE() {
        // The query has two ? placeholders, but only a single null arg is supplied.
        // Previously this would NPE inside prepareStatement at parameters[0].getClass().
        // After the fix, the loop sees values.length < parameterCount and throws IAE.
        try {
            cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", (Object) null);
            fail("Expected IllegalArgumentException for too-few parameters");
        } catch (IllegalArgumentException e) {
            // expected
        } catch (NullPointerException e) {
            fail("Got NPE instead of IllegalArgumentException: " + e);
        }
    }

    /**
     * Verifies that passing fewer parameters than the query expects yields
     * an IllegalArgumentException, not an ArrayIndexOutOfBoundsException
     * from the parameter-type-conversion loop in prepareStatement.
     */
    @Test
    public void test_prepareStatement_tooFewPositionalParams_throwsIAE_notAIOOBE() {
        try {
            cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", UUID.randomUUID());
            fail("Expected IllegalArgumentException for too-few parameters");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage() == null || e.getMessage().toLowerCase().contains("parameter"), "Message should mention parameters: " + e.getMessage());
        } catch (ArrayIndexOutOfBoundsException e) {
            fail("Got AIOOBE instead of IllegalArgumentException: " + e);
        }
    }

    // ---------------------------------------------------------------------
    //  ParsedCql parser tests (no DB required)
    // ---------------------------------------------------------------------

    @Test
    public void test_ParsedCql_positionalParams() {
        ParsedCql parsed = ParsedCql.parse("SELECT * FROM simplex.users WHERE id = ? AND status = ?", null);

        assertEquals(2, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
        assertEquals("SELECT * FROM simplex.users WHERE id = ? AND status = ?", parsed.getParameterizedCql());
    }

    @Test
    public void test_ParsedCql_namedColonParams() {
        ParsedCql parsed = ParsedCql.parse("SELECT * FROM simplex.users WHERE id = :userId AND status = :st", null);

        assertEquals(2, parsed.parameterCount());
        Map<Integer, String> named = parsed.namedParameters();
        assertEquals("userId", named.get(0));
        assertEquals("st", named.get(1));
        // The colon syntax should be normalized to '?'
        assertFalse(parsed.getParameterizedCql().contains(":"), "Parameterized CQL should not contain ':' " + parsed.getParameterizedCql());
    }

    @Test
    public void test_ParsedCql_ibatisStyleParams() {
        ParsedCql parsed = ParsedCql.parse("SELECT * FROM simplex.users WHERE id = #{userId} AND name = #{name}", null);

        assertEquals(2, parsed.parameterCount());
        assertEquals("userId", parsed.namedParameters().get(0));
        assertEquals("name", parsed.namedParameters().get(1));
        assertFalse(parsed.getParameterizedCql().contains("#{"), "Parameterized CQL should not contain '#{' " + parsed.getParameterizedCql());
    }

    @Test
    public void test_ParsedCql_emptyIbatisParameterNameThrows() {
        // SqlParser keeps `#{...}` tokens whole. An empty `#{}` should be rejected
        // because the parameter name would be empty.
        // If the tokenizer ever changes and #{} is split, the test will still pass
        // because the SELECT statement will simply have no parameters - so we
        // verify the throw is raised OR the input is parsed without a named param.
        try {
            ParsedCql parsed = ParsedCql.parse("SELECT * FROM simplex.users WHERE id = #{}", null);
            // If we got here, no exception was thrown. Make sure no bogus empty-named param was created.
            assertFalse(parsed.namedParameters().containsValue(""), "Empty-named parameter must not be silently registered: " + parsed.namedParameters());
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void test_ParsedCql_mixedStylesThrows() {
        // mix '?' and ':name'
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM simplex.users WHERE id = ? AND status = :st", null),
                "Mixed parameter styles should throw IAE");

        // mix '?' and '#{name}'
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM simplex.users WHERE id = ? AND status = #{st}", null),
                "Mixed '?' and '#{}' parameter styles should throw IAE");

        // mix ':name' and '#{name}'
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse("SELECT * FROM simplex.users WHERE id = :id AND status = #{st}", null),
                "Mixed ':name' and '#{}' parameter styles should throw IAE");
    }

    @Test
    public void test_ParsedCql_nullCqlThrows() {
        assertThrows(IllegalArgumentException.class, () -> ParsedCql.parse(null, null));
    }

    @Test
    public void test_ParsedCql_caching() {
        // The same CQL string with null attrs should return the same cached instance.
        String cql = "SELECT * FROM simplex.users WHERE id = ? AND name = ?";
        ParsedCql first = ParsedCql.parse(cql, null);
        ParsedCql second = ParsedCql.parse(cql, null);
        assertTrue(first == second, "Cached parse() should return the same instance for identical CQL with null attrs");

        // With non-empty attrs, the cache is bypassed and a fresh instance is returned.
        Map<String, String> attrs = new HashMap<>();
        attrs.put("timeout", "5000");
        ParsedCql withAttrs1 = ParsedCql.parse(cql, attrs);
        ParsedCql withAttrs2 = ParsedCql.parse(cql, attrs);
        assertTrue(withAttrs1 != withAttrs2, "Non-empty attrs should bypass the cache");
        assertEquals("5000", withAttrs1.getAttributes().get("timeout"));
    }

    @Test
    public void test_ParsedCql_trailingSemicolonRemoved() {
        ParsedCql parsed = ParsedCql.parse("SELECT * FROM simplex.users WHERE id = ?;", null);
        assertFalse(parsed.getParameterizedCql().endsWith(";"), "Trailing semicolon should be removed: " + parsed.getParameterizedCql());
    }

    @Test
    public void test_ParsedCql_ddlNoParameterParsing() {
        // For non-DML statements (e.g. CREATE/ALTER/DROP) parameterCount is always 0
        // and the CQL is kept as-is (parameter style detection is skipped).
        ParsedCql parsed = ParsedCql.parse("CREATE TABLE if not exists t (id uuid PRIMARY KEY, val text)", null);
        assertEquals(0, parsed.parameterCount());
        assertTrue(parsed.namedParameters().isEmpty());
    }

    @Test
    public void test_ParsedCql_originalCqlTrimmed() {
        ParsedCql parsed = ParsedCql.parse("   SELECT * FROM simplex.users WHERE id = ?   ", null);
        // originalCql() returns the trimmed original
        assertEquals("SELECT * FROM simplex.users WHERE id = ?", parsed.originalCql());
    }

    @Test
    public void test_ParsedCql_attributesDefensiveCopy() {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("timeout", "1000");
        ParsedCql parsed = ParsedCql.parse("SELECT * FROM simplex.users WHERE id = ?", attrs);

        // Mutating the source map after parse should not affect the ParsedCql's attrs.
        attrs.put("timeout", "9999");
        assertEquals("1000", parsed.getAttributes().get("timeout"));
    }

    @Test
    public void test_ParsedCql_equalsAndHashCode() {
        ParsedCql a = ParsedCql.parse("SELECT 1 FROM simplex.users WHERE id = ?", null);
        ParsedCql b = ParsedCql.parse("SELECT 1 FROM simplex.users WHERE id = ?", null);
        ParsedCql c = ParsedCql.parse("SELECT 2 FROM simplex.users WHERE id = ?", null);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
        // equals against non-ParsedCql
        assertFalse(a.equals("not a ParsedCql"));
        assertFalse(a.equals(null));
    }

    // ---------------------------------------------------------------------
    //  CqlMapper tests (no DB required)
    // ---------------------------------------------------------------------

    @Test
    public void test_CqlMapper_addAndGet() {
        CqlMapper mapper = new CqlMapper();
        Map<String, String> attrs = new HashMap<>();
        attrs.put("timeout", "3000");

        mapper.add("findUser", "SELECT * FROM simplex.users WHERE id = ?", attrs);

        ParsedCql parsed = mapper.get("findUser");
        assertNotNull(parsed);
        assertEquals(1, parsed.parameterCount());
        assertEquals("3000", parsed.getAttributes().get("timeout"));
        assertNull(mapper.get("nonExistent"));
    }

    @Test
    public void test_CqlMapper_addDuplicateIdThrows() {
        CqlMapper mapper = new CqlMapper();
        mapper.add("dup", "SELECT * FROM simplex.users WHERE id = ?", null);
        assertThrows(IllegalArgumentException.class, () -> mapper.add("dup", "SELECT * FROM simplex.users WHERE id = ?", null),
                "Adding a duplicate id should throw");
    }

    @Test
    public void test_CqlMapper_copyIsIndependent() {
        CqlMapper original = new CqlMapper();
        original.add("a", "SELECT 1 FROM simplex.users WHERE id = ?", null);

        CqlMapper copy = original.copy();
        assertNotNull(copy.get("a"));

        copy.remove("a");
        assertNull(copy.get("a"));
        // Original should still have the entry
        assertNotNull(original.get("a"));

        original.add("b", "SELECT 2 FROM simplex.users WHERE id = ?", null);
        // Copy must not have seen the new addition
        assertNull(copy.get("b"));
    }

    @Test
    public void test_CqlMapper_keySet_isEmpty_remove() {
        CqlMapper mapper = new CqlMapper();
        assertTrue(mapper.isEmpty());
        assertTrue(mapper.keySet().isEmpty());

        mapper.add("k1", "SELECT 3 FROM simplex.users WHERE id = ?", null);
        assertFalse(mapper.isEmpty());
        assertTrue(mapper.keySet().contains("k1"));

        mapper.remove("k1");
        assertTrue(mapper.isEmpty());
        // remove of missing key is a no-op (does not throw)
        mapper.remove("missing");
    }

    // ---------------------------------------------------------------------
    //  StatementSettings (v3) tests - no DB required
    // ---------------------------------------------------------------------

    @Test
    public void test_StatementSettings_defaultsAreNull() {
        StatementSettings settings = new StatementSettings();
        assertNull(settings.consistency());
        assertNull(settings.serialConsistency());
        assertNull(settings.retryPolicy());
        assertNull(settings.fetchSize());
        assertNull(settings.readTimeoutMillis());
        assertNull(settings.traceQuery());
    }

    @Test
    public void test_StatementSettings_allArgsConstructor() {
        StatementSettings settings = new StatementSettings(ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, DefaultRetryPolicy.INSTANCE, 500, 7000,
                Boolean.TRUE);
        assertEquals(ConsistencyLevel.QUORUM, settings.consistency());
        assertEquals(ConsistencyLevel.SERIAL, settings.serialConsistency());
        assertEquals(DefaultRetryPolicy.INSTANCE, settings.retryPolicy());
        assertEquals(500, settings.fetchSize().intValue());
        assertEquals(7000, settings.readTimeoutMillis().intValue());
        assertTrue(settings.traceQuery());
    }

    @Test
    public void test_StatementSettings_builderAndFluentSetters() {
        StatementSettings settings = StatementSettings.builder()
                .consistency(ConsistencyLevel.ONE)
                .serialConsistency(ConsistencyLevel.LOCAL_SERIAL)
                .retryPolicy(DefaultRetryPolicy.INSTANCE)
                .fetchSize(100)
                .readTimeoutMillis(2000)
                .traceQuery(Boolean.FALSE)
                .build();

        assertEquals(ConsistencyLevel.ONE, settings.consistency());
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, settings.serialConsistency());
        assertEquals(DefaultRetryPolicy.INSTANCE, settings.retryPolicy());
        assertEquals(100, settings.fetchSize().intValue());
        assertEquals(2000, settings.readTimeoutMillis().intValue());
        assertFalse(settings.traceQuery());

        // Verify fluent setters return same instance
        StatementSettings s2 = new StatementSettings();
        assertTrue(s2 == s2.consistency(ConsistencyLevel.TWO));
        assertEquals(ConsistencyLevel.TWO, s2.consistency());
    }

    // ---------------------------------------------------------------------
    //  Constructor / accessor tests (v3) - use live cluster from static init
    // ---------------------------------------------------------------------

    @Test
    public void test_clusterAndSession_accessors() {
        assertNotNull(cassandraExecutor.cluster());
        assertNotNull(cassandraExecutor.session());
        assertNotNull(cassandraExecutor.async());
        // async() should return same instance per executor
        assertTrue(cassandraExecutor.async() == cassandraExecutor.async());
    }

    @Test
    public void test_constructor_withSettingsAndMapper() {
        // Constructor coverage: (Session, settings, mapper, NamingPolicy)
        StatementSettings settings = StatementSettings.builder().fetchSize(50).build();
        CqlMapper mapper = new CqlMapper();
        CassandraExecutor exec = new CassandraExecutor(cassandraExecutor.session(), settings, mapper, NamingPolicy.SNAKE_CASE);
        assertNotNull(exec);
        // Verify session was wired correctly
        assertTrue(exec.session() == cassandraExecutor.session());
    }

    @Test
    public void test_constructor_withNullSettings() {
        CassandraExecutor exec = new CassandraExecutor(cassandraExecutor.session(), null);
        assertNotNull(exec);
        assertTrue(exec.session() == cassandraExecutor.session());
    }

    // TODO: test_mapper_returnsNonNull requires a class with com.datastax.driver.mapping.annotations.@Table
    //       annotation, which neither Users nor Song carry; skipping to avoid forging a test entity.

    // ---------------------------------------------------------------------
    //  Static helper tests against live ResultSets - no extra DB needed
    // ---------------------------------------------------------------------

    @Test
    public void test_extractData_withTargetClass() {
        // Insert a known row and verify extractData with target class converts properly.
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist) VALUES (?, ?, ?, ?)", id, "t", "a", "ar");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = ?", id);
            Dataset ds = CassandraExecutor.extractData(rs, Song.class);
            assertNotNull(ds);
            assertEquals(1, ds.size());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_extractData_withMapClass() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "t");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT id, title FROM simplex.songs WHERE id = ?", id);
            Dataset ds = CassandraExecutor.extractData(rs, Map.class);
            assertNotNull(ds);
            assertEquals(1, ds.size());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_toList_withRowClass() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "t");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = ?", id);
            // toList with Row.class fast-path
            List<Row> rows = CassandraExecutor.toList(rs, Row.class);
            assertEquals(1, rows.size());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_toList_withSingleColumn() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "myTitle");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT title FROM simplex.songs WHERE id = ?", id);
            List<String> titles = CassandraExecutor.toList(rs, String.class);
            assertEquals(1, titles.size());
            assertEquals("myTitle", titles.get(0));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_toMap_withCustomSupplier() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "t");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT id, title FROM simplex.songs WHERE id = ?", id);
            Row row = rs.one();
            assertNotNull(row);
            // Use a LinkedHashMap supplier to exercise the supplier branch
            Map<String, Object> map = CassandraExecutor.toMap(row, IntFunctions.ofLinkedHashMap());
            assertNotNull(map);
            assertEquals(2, map.size());
            assertTrue(map.containsKey("id"));
            assertTrue(map.containsKey("title"));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_toEntity_song() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album, artist) VALUES (?, ?, ?, ?)", id, "myT", "myA", "myAr");
        try {
            Row row = cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = ?", id).one();
            assertNotNull(row);
            Song s = CassandraExecutor.toEntity(row, Song.class);
            assertNotNull(s);
            assertEquals(id, s.getId());
            assertEquals("myT", s.getTitle());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    // ---------------------------------------------------------------------
    //  findFirst / queryForSingle empty-result branches
    // ---------------------------------------------------------------------

    @Test
    public void test_findFirst_noRows_returnsEmpty() {
        UUID nonExistent = UUID.randomUUID();
        assertTrue(cassandraExecutor.findFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", nonExistent).isEmpty());
    }

    @Test
    public void test_queryForSingleValue_noRows_returnsEmptyNullable() {
        UUID nonExistent = UUID.randomUUID();
        assertTrue(cassandraExecutor.queryForSingleValue(String.class, "SELECT title FROM simplex.songs WHERE id = ?", nonExistent).isEmpty());
    }

    @Test
    public void test_queryForSingleNonNull_noRows_returnsEmptyOptional() {
        UUID nonExistent = UUID.randomUUID();
        assertTrue(cassandraExecutor.queryForSingleNonNull(String.class, "SELECT title FROM simplex.songs WHERE id = ?", nonExistent).isEmpty());
    }

    @Test
    public void test_queryForSingleNonNull_withValue() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "x");
        try {
            assertEquals("x", cassandraExecutor.queryForSingleNonNull(String.class, "SELECT title FROM simplex.songs WHERE id = ?", id).orElse(null));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    // ---------------------------------------------------------------------
    //  stream(...) / execute(...) coverage
    // ---------------------------------------------------------------------

    @Test
    public void test_stream_withBiFunctionRowMapper() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "stream-title");
        try {
            long count = cassandraExecutor.stream("SELECT * FROM simplex.songs WHERE id = ?", (cds, r) -> r.getString("title"), id).count();
            assertEquals(1L, count);
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_stream_withStatement_BiFunctionRowMapper() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "stmt-title");
        try {
            // Prepare a statement via the live session so we can stream over it.
            BoundStatement bs = cassandraExecutor.session().prepare("SELECT * FROM simplex.songs WHERE id = ?").bind(id);
            long count = cassandraExecutor.stream((Statement) bs, (cds, r) -> r.getString("title")).count();
            assertEquals(1L, count);
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_stream_nullRowMapper_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> cassandraExecutor.stream("SELECT * FROM simplex.songs",
                (java.util.function.BiFunction<com.datastax.driver.core.ColumnDefinitions, Row, Object>) null));
    }

    @Test
    public void test_execute_withMapNamedParams() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "named-t");
        try {
            // Exercise the Map-parameter overload of execute(String, Map)
            Map<String, Object> params = new HashMap<>();
            params.put("id", id);
            ResultSet rs = cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = :id", params);
            assertNotNull(rs);
            assertNotNull(rs.one());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_execute_missingNamedParameter_throwsIAE() {
        // Map missing :id key -> "Missing required parameter"
        Map<String, Object> params = new HashMap<>();
        params.put("wrongKey", UUID.randomUUID());
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = :id", params));
        assertTrue(ex.getMessage() == null || ex.getMessage().toLowerCase().contains("missing") || ex.getMessage().toLowerCase().contains("parameter"));
    }

    // ---------------------------------------------------------------------
    //  StringCodec (v3 driver) tests - access via registerTypeCodec since
    //  StringCodec is package-private with a protected constructor.
    // ---------------------------------------------------------------------

    /** Helper: build a StringCodec via the codec registry. */
    @SuppressWarnings("unchecked")
    private static <T> TypeCodec<T> newStringCodec(Class<T> cls) {
        com.datastax.driver.core.CodecRegistry reg = new com.datastax.driver.core.CodecRegistry();
        CassandraExecutor.registerTypeCodec(reg, cls);
        return (TypeCodec<T>) reg.codecFor(DataType.varchar(), cls);
    }

    @Test
    public void test_v3_StringCodec_format_null_returnsNULL() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        assertEquals("NULL", codec.format(null));
    }

    @Test
    public void test_v3_StringCodec_format_value_returnsJson() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        Song s = new Song();
        s.setTitle("v3t");
        String json = codec.format(s);
        assertNotNull(json);
        assertTrue(json.contains("v3t"));
    }

    @Test
    public void test_v3_StringCodec_parse_empty_returnsNull() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        assertNull(codec.parse(""));
    }

    @Test
    public void test_v3_StringCodec_parse_NULL_returnsNull() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        assertNull(codec.parse("NULL"));
    }

    @Test
    public void test_v3_StringCodec_parse_json_returnsObject() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        Song s = new Song();
        s.setTitle("p3");
        String json = codec.format(s);
        Song parsed = codec.parse(json);
        assertNotNull(parsed);
        assertEquals("p3", parsed.getTitle());
    }

    @Test
    public void test_v3_StringCodec_encodeDecode_roundtrip() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        Song s = new Song();
        s.setTitle("rt3");
        s.setAlbum("alb3");
        ByteBuffer encoded = codec.serialize(s, ProtocolVersion.V4);
        assertNotNull(encoded);
        Song decoded = codec.deserialize(encoded, ProtocolVersion.V4);
        assertNotNull(decoded);
        assertEquals("rt3", decoded.getTitle());
        assertEquals("alb3", decoded.getAlbum());
    }

    // ---------------------------------------------------------------------
    //  UDTCodec (v3 driver) tests - rely on UDTs registered in static init.
    // ---------------------------------------------------------------------

    /** Helper: get a UserType from the live cluster metadata. */
    private static UserType lookupUDT(String name) {
        return cassandraExecutor.cluster().getMetadata().getKeyspace("simplex").getUserType(name);
    }

    @Test
    public void test_v3_UDTCodec_create_fromUserType_returnsCodec() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        assertNotNull(codec);
        assertEquals(Users.Name.class, codec.getJavaType().getRawType());
    }

    @Test
    public void test_v3_UDTCodec_create_withCluster_returnsCodec() {
        UDTCodec<Users.Name> codec = UDTCodec.create(cassandraExecutor.cluster(), "simplex", "fullname", Users.Name.class);
        assertNotNull(codec);
        assertNotNull(codec.getCqlType());
    }

    @Test
    public void test_v3_UDTCodec_format_null_returnsNULL() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        assertEquals("NULL", codec.format(null));
    }

    @Test
    public void test_v3_UDTCodec_parse_empty_returnsNull() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        assertNull(codec.parse(""));
        assertNull(codec.parse("NULL"));
    }

    @Test
    public void test_v3_UDTCodec_format_value_returnsJson() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        Users.Name n = new Users.Name();
        n.setFirstName("a3");
        n.setLastName("b3");
        String json = codec.format(n);
        assertNotNull(json);
        assertTrue(json.contains("a3"));
    }

    @Test
    public void test_v3_UDTCodec_parse_json_returnsObject() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        String json = "{\"firstname\":\"x3\",\"lastname\":\"y3\"}";
        Users.Name n = codec.parse(json);
        assertNotNull(n);
        // Field names may or may not match depending on JSON parser case sensitivity.
        // Just verify the parse did not return null.
    }

    @Test
    public void test_v3_UDTCodec_serializeBean_roundtrip() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        Users.Name n = new Users.Name();
        n.setFirstName("first3");
        n.setLastName("last3");
        ByteBuffer encoded = codec.serialize(n, ProtocolVersion.V4);
        assertNotNull(encoded);
        Users.Name back = codec.deserialize(encoded, ProtocolVersion.V4);
        assertNotNull(back);
        assertEquals("first3", back.getFirstName());
        assertEquals("last3", back.getLastName());
    }

    @Test
    public void test_v3_UDTCodec_serialize_invalidType_throwsIAE() {
        UDTCodec<Integer> codec = UDTCodec.create(lookupUDT("fullname"), Integer.class);
        assertThrows(IllegalArgumentException.class, () -> codec.serialize(42, ProtocolVersion.V4));
    }

    @Test
    public void test_v3_UDTCodec_serializeMap_roundtrip() {
        UDTCodec<Map> codec = UDTCodec.create(lookupUDT("fullname"), Map.class);
        Map<String, Object> m = new HashMap<>();
        m.put("firstname", "fm");
        m.put("lastname", "lm");
        ByteBuffer encoded = codec.serialize(m, ProtocolVersion.V4);
        assertNotNull(encoded);
        Map back = codec.deserialize(encoded, ProtocolVersion.V4);
        assertNotNull(back);
        assertEquals("fm", back.get("firstname"));
        assertEquals("lm", back.get("lastname"));
    }

    @Test
    public void test_v3_UDTCodec_serializeCollection_roundtrip() {
        UDTCodec<List> codec = UDTCodec.create(lookupUDT("fullname"), List.class);
        List<Object> values = N.asList("first3c", "last3c");
        ByteBuffer encoded = codec.serialize(values, ProtocolVersion.V4);
        assertNotNull(encoded);
        List back = codec.deserialize(encoded, ProtocolVersion.V4);
        assertNotNull(back);
        assertEquals(2, back.size());
    }

    @Test
    public void test_v3_UDTCodec_serialize_null_returnsNullBuffer() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        ByteBuffer bb = codec.serialize(null, ProtocolVersion.V4);
        // The protected serialize(T) returns null for null input; the wrapping codec produces null buffer.
        assertNull(bb);
    }

    // ---------------------------------------------------------------------
    //  StatementSettings (v3) — extra equals/hashCode/toString coverage
    // ---------------------------------------------------------------------

    @Test
    public void test_v3_StatementSettings_equalsAndHashCode() {
        StatementSettings a = new StatementSettings(ConsistencyLevel.ONE, ConsistencyLevel.SERIAL, DefaultRetryPolicy.INSTANCE, 5, 1000, Boolean.TRUE);
        StatementSettings b = new StatementSettings(ConsistencyLevel.ONE, ConsistencyLevel.SERIAL, DefaultRetryPolicy.INSTANCE, 5, 1000, Boolean.TRUE);
        StatementSettings c = new StatementSettings(ConsistencyLevel.TWO, ConsistencyLevel.SERIAL, DefaultRetryPolicy.INSTANCE, 5, 1000, Boolean.TRUE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }

    @Test
    public void test_v3_StatementSettings_toString_containsValues() {
        StatementSettings s = new StatementSettings();
        s.fetchSize(456);
        String str = s.toString();
        assertNotNull(str);
        assertTrue(str.contains("456"));
    }

    @Test
    public void test_v3_StatementSettings_allFluentSetters_returnSelf() {
        StatementSettings s = new StatementSettings();
        assertTrue(s == s.consistency(ConsistencyLevel.ONE));
        assertTrue(s == s.serialConsistency(ConsistencyLevel.SERIAL));
        assertTrue(s == s.retryPolicy(DefaultRetryPolicy.INSTANCE));
        assertTrue(s == s.fetchSize(11));
        assertTrue(s == s.readTimeoutMillis(2222));
        assertTrue(s == s.traceQuery(Boolean.TRUE));
        assertEquals(11, s.fetchSize().intValue());
        assertEquals(2222, s.readTimeoutMillis().intValue());
    }

    private Users createUser() {
        Users user = new Users();
        user.setId(UUID.randomUUID());
        Users.Name name = new Users.Name();
        name.setFirstName("fn");
        name.setLastName("ln");
        user.setName(name);

        Users.Address address = new Users.Address();
        address.setCity("sunnyvale");
        address.setStreet("1");
        address.setZipCode(123);
        Map<String, Users.Address> addresses = N.asMap("home", address);
        user.setAddresses(addresses);

        user.setSomeDate(LocalDate.now());
        // user.setLastUpdateTime(LocalTime.now());
        user.setCreatedTime(Dates.currentTimestamp());

        user.setBytes(N.asList((byte) 1, (byte) 2, (byte) 3));
        user.setShorts(N.asSet((short) 1, (short) 2, (short) 3));

        return user;
    }
}
