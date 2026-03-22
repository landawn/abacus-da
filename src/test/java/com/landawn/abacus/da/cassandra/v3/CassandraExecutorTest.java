/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
import com.landawn.abacus.da.AbstractNoSQLTest;
import com.landawn.abacus.da.cassandra.CqlBuilder.LCCB;
import com.landawn.abacus.da.cassandra.CqlBuilder.NLC;
import com.landawn.abacus.da.cassandra.CqlBuilder.NSC;
import com.landawn.abacus.da.cassandra.CqlBuilder.SCCB;
import com.landawn.abacus.da.cassandra.v3.CassandraExecutor.UDTCodec;
import com.landawn.abacus.da.entity.Song;
import com.landawn.abacus.da.entity.Users;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.Dataset;
import com.landawn.abacus.util.Dates;
import com.landawn.abacus.util.Fn;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.stream.Stream;

public class CassandraExecutorTest extends AbstractNoSQLTest {

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

        List<Users> dbUsers = cassandraExecutor.asyncList(Users.class, "SELECT * FROM simplex.users").get();
        N.println(dbUsers);
        cassandraExecutor.asyncDelete(Users.class, Filters.in("id", N.map(dbUsers, Users::getId)));
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

        cassandraExecutor.queryForSingleResult(Users.class, Users.Name.class, "name", Filters.eq("id", id)).ifPresent(Fn.println());
        assertFalse(cassandraExecutor.queryForSingleResult(Users.class, Users.Name.class, "name", Filters.eq("id", id)).isNull());
        cassandraExecutor.execute(cql, id);
        assertTrue(cassandraExecutor.queryForSingleResult(Users.class, Users.Name.class, "name", Filters.eq("id", id)).isNull());

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

        cassandraExecutor.asyncBatchDelete(dbUsers).get();

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

        UUID uuid = cassandraExecutor.queryForSingleResult(UUID.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
        N.println(uuid);
        assertEquals(song.getId(), uuid);

        String strUUID = cassandraExecutor.queryForSingleResult(String.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).orElse(null);
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

        assertTrue(cassandraExecutor.asyncExists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get());

        assertEquals(1, cassandraExecutor.asyncCount("SELECT count(*) FROM simplex.songs WHERE id = ?", song.getId()).get().longValue());

        Song dbSong = cassandraExecutor.asyncFindFirst(Song.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get().orElse(null);
        N.println(dbSong);
        assertEquals(song.getId(), dbSong.getId());

        Map<String, Object> map = cassandraExecutor.asyncFindFirst(Map.class, "SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get().orElse(null);
        N.println(map);
        assertEquals(song.getId(), map.get("id"));

        Dataset dataset = cassandraExecutor.asyncQuery("SELECT * FROM simplex.songs WHERE id = 756716f7-2e54-4715-9f00-91dcbea6cf50;").get();
        dataset.println();
        assertEquals(song.getId(), dataset.get("id"));

        UUID uuid = cassandraExecutor.asyncQueryForSingleResult(UUID.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId()).get().orElse(null);
        N.println(uuid);
        assertEquals(song.getId(), uuid);

        String strUUID = cassandraExecutor.asyncQueryForSingleResult(String.class, "SELECT id FROM simplex.songs WHERE id = ?", song.getId())
                .get()
                .orElse(null);
        N.println(strUUID);
        assertEquals(song.getId().toString(), strUUID);

        assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()));

        cassandraExecutor.asyncExecute("DELETE FROM simplex.songs WHERE id = ?", song.getId()).get();
        assertFalse(cassandraExecutor.asyncExists("SELECT * FROM simplex.songs WHERE id = ?", song.getId()).get());
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
