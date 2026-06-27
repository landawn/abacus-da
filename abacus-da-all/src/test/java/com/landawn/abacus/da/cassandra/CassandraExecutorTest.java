/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da.cassandra;

import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.LCCB;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NLC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.NSC;
import static com.landawn.abacus.da.cassandra.CqlBuilder.Dsl.SCCB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.cassandra.CassandraExecutor.StatementSettings;
import com.landawn.abacus.da.cassandra.CassandraExecutor.UDTCodec;
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
    private static final MutableCodecRegistry codecRegistry = new DefaultCodecRegistry("xxx");

    static final CqlSession session = CqlSession.builder().withCodecRegistry(codecRegistry).build();
    static final CassandraExecutor cassandraExecutor;

    static {
        cassandraExecutor = new CassandraExecutor(session);

        //    final String queries = """
        //            CREATE KEYSPACE IF NOT EXISTS simplex WITH replication  = {'class':'SimpleStrategy', 'replication_factor':3};
        //            DROP TYPE IF EXISTS simplex.fullname;
        //            DROP TYPE IF EXISTS simplex.address;
        //            DROP TABLE IF EXISTS simplex.users;
        //            CREATE TYPE IF NOT EXISTS simplex.fullname(firstName text, lastName text);
        //            CREATE TYPE IF NOT EXISTS simplex.address(street text, city text, zipCode int);
        //            CREATE TABLE IF NOT EXISTS simplex.users(id uuid PRIMARY KEY, name frozen <fullname>, addresses map<text, frozen <address>>, someDate varchar, last_update_time time, created_time timestamp, bytes list<tinyint>, shorts set<smallint>);
        //            CREATE TABLE IF NOT EXISTS simplex.users(id uuid PRIMARY KEY, name text, addresses text);
        //
        //            CREATE TABLE IF NOT EXISTS simplex.songs (id uuid PRIMARY KEY, title text, album text, artist text, tags set<text>, data blob);
        //            CREATE TABLE IF NOT EXISTS simplex.playlists (id uuid, title text, album text,  artist text, song_id uuid, PRIMARY KEY (id, title, album, artist));
        //                        """;
        //
        //    StreamEx.split(queries, ";").map(Fn.strip()).peek(Fn.println()).forEach(query -> Try.run(() -> cassandraExecutor.execute(query), Fn.doNothing()));

        codecRegistry.register(UDTCodec.create(session, "simplex", "fullname", Users.Name.class));

        codecRegistry.register(UDTCodec.create(session, "simplex", "address", Users.Address.class));

        cassandraExecutor.registerTypeCodec(Timestamp.class);
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
        cassandraExecutor.batchInsert(users, BatchType.LOGGED);

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
        cassandraExecutor.batchInsert(users, BatchType.LOGGED);

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
        cassandraExecutor.batchInsert(users, BatchType.LOGGED);

        List<UUID> ids = cassandraExecutor.list(UUID.class, "SELECT id FROM simplex.users");
        N.println(ids);

        List<Users> dbUsers = cassandraExecutor.list(Users.class, "SELECT * FROM simplex.users");

        dbUsers.stream().forEach(it -> it.getName().setFirstName("updatedFirstName"));

        cassandraExecutor.batchUpdate(dbUsers, BatchType.UNLOGGED);

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

    // ---------------------------------------------------------------------
    //  StatementSettings (v2) tests - no DB required
    // ---------------------------------------------------------------------

    @Test
    public void test_StatementSettings_defaultsAreNull() {
        StatementSettings settings = new StatementSettings();
        assertNull(settings.consistency());
        assertNull(settings.serialConsistency());
        assertNull(settings.fetchSize());
        assertNull(settings.timeout());
        assertNull(settings.traceQuery());
    }

    @Test
    public void test_StatementSettings_allArgsConstructorAndBuilder() {
        java.time.Duration d = java.time.Duration.ofSeconds(15);
        StatementSettings s1 = new StatementSettings(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_SERIAL, 250, d, Boolean.TRUE);
        assertEquals(ConsistencyLevel.QUORUM, s1.consistency());
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, s1.serialConsistency());
        assertEquals(250, s1.fetchSize().intValue());
        assertEquals(d, s1.timeout());
        assertTrue(s1.traceQuery());

        StatementSettings s2 = StatementSettings.builder()
                .consistency(ConsistencyLevel.ONE)
                .serialConsistency(ConsistencyLevel.SERIAL)
                .fetchSize(99)
                .timeout(java.time.Duration.ofMillis(500))
                .traceQuery(Boolean.FALSE)
                .build();
        assertEquals(ConsistencyLevel.ONE, s2.consistency());
        assertEquals(99, s2.fetchSize().intValue());
        assertFalse(s2.traceQuery());
    }

    // ---------------------------------------------------------------------
    //  Constructor / accessor coverage
    // ---------------------------------------------------------------------

    @Test
    public void test_constructor_withSettingsAndMapper() {
        StatementSettings settings = StatementSettings.builder().fetchSize(50).build();
        CqlMapper mapper = new CqlMapper();
        CassandraExecutor exec = new CassandraExecutor(cassandraExecutor.session(), settings, mapper, NamingPolicy.SNAKE_CASE);
        assertNotNull(exec);
        assertTrue(exec.session() == cassandraExecutor.session());
        assertNotNull(exec.async());
    }

    @Test
    public void test_constructor_withSettingsOnly() {
        CassandraExecutor exec = new CassandraExecutor(cassandraExecutor.session(), null);
        assertNotNull(exec);
        assertNotNull(exec.async());
    }

    @Test
    public void test_async_returnsSameInstance() {
        // async() should return the same AsyncCassandraExecutor every time
        assertTrue(cassandraExecutor.async() == cassandraExecutor.async());
    }

    // ---------------------------------------------------------------------
    //  Static helper tests against live ResultSets
    // ---------------------------------------------------------------------

    @Test
    public void test_extractData_withEntityClass() {
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
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "rowTest");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = ?", id);
            // Fast-path: target is Row.class -> returns resultSet.all() directly.
            List<Row> rows = CassandraExecutor.toList(rs, Row.class);
            assertEquals(1, rows.size());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_toList_singleColumnValue() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "scalar");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT title FROM simplex.songs WHERE id = ?", id);
            List<String> titles = CassandraExecutor.toList(rs, String.class);
            assertEquals(1, titles.size());
            assertEquals("scalar", titles.get(0));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_toMap_withSupplier() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "mapT");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT id, title FROM simplex.songs WHERE id = ?", id);
            Row row = rs.one();
            assertNotNull(row);
            // Use a LinkedHashMap supplier to verify the supplier branch is exercised.
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
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title, album) VALUES (?, ?, ?)", id, "t1", "a1");
        try {
            Row row = cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = ?", id).one();
            assertNotNull(row);
            Song s = CassandraExecutor.toEntity(row, Song.class);
            assertNotNull(s);
            assertEquals(id, s.getId());
            assertEquals("t1", s.getTitle());
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
    //  stream(...) coverage
    // ---------------------------------------------------------------------

    @Test
    public void test_stream_withBiFunctionRowMapper() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "sTitle");
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
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "stmtT");
        try {
            BoundStatement bs = cassandraExecutor.session().prepare("SELECT * FROM simplex.songs WHERE id = ?").bind(id);
            long count = cassandraExecutor.stream((Statement<?>) bs, (cds, r) -> r.getString("title")).count();
            assertEquals(1L, count);
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_stream_nullRowMapper_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> cassandraExecutor.stream("SELECT * FROM simplex.songs",
                (java.util.function.BiFunction<com.datastax.oss.driver.api.core.cql.ColumnDefinitions, Row, Object>) null));
    }

    @Test
    public void test_execute_withMapNamedParams() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "nT");
        try {
            Map<String, Object> params = new java.util.HashMap<>();
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
        Map<String, Object> params = new java.util.HashMap<>();
        params.put("wrong", UUID.randomUUID());
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = :id", params));
        assertTrue(ex.getMessage() == null || ex.getMessage().toLowerCase().contains("missing") || ex.getMessage().toLowerCase().contains("parameter"));
    }

    /**
     * Verifies that passing a single {@code null} parameter when the query
     * expects multiple parameters produces an IllegalArgumentException
     * (not a NullPointerException from {@code parameters[0].getClass()} and
     * not an ArrayIndexOutOfBoundsException from the parameter-conversion loop).
     */
    @Test
    public void test_prepareStatement_nullSingleParamMultiParamQuery_throwsIAE_notNPE() {
        // The query has two ? placeholders, but only a single null arg is supplied.
        // Previously this would NPE inside prepareStatement at parameters[0].getClass(),
        // and even after the null-guard it would AIOOBE in the value-conversion loop.
        // After the fix, values.length < parameterCount throws IAE.
        try {
            cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", (Object) null);
            fail("Expected IllegalArgumentException for too-few parameters");
        } catch (IllegalArgumentException e) {
            // expected
        } catch (NullPointerException e) {
            fail("Got NPE instead of IllegalArgumentException: " + e);
        } catch (ArrayIndexOutOfBoundsException e) {
            fail("Got AIOOBE instead of IllegalArgumentException: " + e);
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
    //  StringCodec (v4 driver) tests - access via registerTypeCodec since
    //  StringCodec is package-private with a protected constructor.
    // ---------------------------------------------------------------------

    /** Helper: build a StringCodec via the codec registry (same as registerTypeCodec). */
    @SuppressWarnings("unchecked")
    private static <T> TypeCodec<T> newStringCodec(Class<T> cls) {
        // Use a fresh registry so this test does not interfere with the static one.
        MutableCodecRegistry reg = new DefaultCodecRegistry("test-" + cls.getSimpleName());
        CassandraExecutor.registerTypeCodec(reg, cls);
        return (TypeCodec<T>) reg.codecFor(DataTypes.TEXT, cls);
    }

    @Test
    public void test_StringCodec_format_null_returnsNULL() {
        TypeCodec<String> codec = newStringCodec(String.class);
        assertEquals("NULL", codec.format(null));
    }

    @Test
    public void test_StringCodec_format_value_returnsJson() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        Song s = new Song();
        s.setTitle("t1");
        String json = codec.format(s);
        assertNotNull(json);
        assertTrue(json.contains("t1"));
    }

    @Test
    public void test_StringCodec_parse_empty_returnsNull() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        assertNull(codec.parse(""));
    }

    @Test
    public void test_StringCodec_parse_NULL_returnsNull() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        assertNull(codec.parse("NULL"));
    }

    @Test
    public void test_StringCodec_parse_json_returnsObject() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        Song s = new Song();
        s.setTitle("hello");
        String json = codec.format(s);
        Song parsed = codec.parse(json);
        assertNotNull(parsed);
        assertEquals("hello", parsed.getTitle());
    }

    @Test
    public void test_StringCodec_encode_null_doesNotThrow() {
        TypeCodec<String> codec = newStringCodec(String.class);
        // encode delegates to TEXT codec on serialize(null) result; just verify it does not throw.
        ByteBuffer bb = codec.encode(null, ProtocolVersion.DEFAULT);
        // Either null or empty buffer is acceptable - both indicate "no value".
        if (bb != null) {
            assertTrue(bb.remaining() >= 0);
        }
    }

    @Test
    public void test_StringCodec_encodeDecode_roundtrip() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        Song s = new Song();
        s.setTitle("rt");
        s.setAlbum("alb");
        ByteBuffer encoded = codec.encode(s, ProtocolVersion.DEFAULT);
        assertNotNull(encoded);
        Song decoded = codec.decode(encoded, ProtocolVersion.DEFAULT);
        assertNotNull(decoded);
        assertEquals("rt", decoded.getTitle());
        assertEquals("alb", decoded.getAlbum());
    }

    @Test
    public void test_StringCodec_getJavaType() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        assertNotNull(codec.getJavaType());
        assertEquals(Song.class, codec.getJavaType().getRawType());
    }

    @Test
    public void test_StringCodec_getCqlType_isText() {
        TypeCodec<Song> codec = newStringCodec(Song.class);
        assertEquals(DataTypes.TEXT, codec.getCqlType());
    }

    @Test
    public void test_StringCodec_deprecated_serializeDeserialize_roundtrip() throws Exception {
        // Exercise the @Deprecated serialize(T, ProtocolVersion) / deserialize(ByteBuffer, ProtocolVersion) overloads via reflection.
        TypeCodec<Song> codec = newStringCodec(Song.class);
        Song s = new Song();
        s.setTitle("dep");
        java.lang.reflect.Method ser = codec.getClass().getMethod("serialize", Object.class, ProtocolVersion.class);
        java.lang.reflect.Method des = codec.getClass().getMethod("deserialize", ByteBuffer.class, ProtocolVersion.class);
        ByteBuffer bb = (ByteBuffer) ser.invoke(codec, s, ProtocolVersion.DEFAULT);
        assertNotNull(bb);
        Song back = (Song) des.invoke(codec, bb, ProtocolVersion.DEFAULT);
        assertNotNull(back);
        assertEquals("dep", back.getTitle());
    }

    // ---------------------------------------------------------------------
    //  UDTCodec (v4 driver) tests - rely on UDTs registered in static init.
    // ---------------------------------------------------------------------

    /** Helper: get a UserDefinedType from the session metadata. */
    private static UserDefinedType lookupUDT(String name) {
        return session.getMetadata().getKeyspace("simplex").orElseThrow().getUserDefinedType(name).orElseThrow();
    }

    @Test
    public void test_UDTCodec_create_fromUserType_returnsCodec() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        assertNotNull(codec);
        assertNotNull(codec.getJavaType());
        assertEquals(Users.Name.class, codec.getJavaType().getRawType());
    }

    @Test
    public void test_UDTCodec_create_withSession_returnsCodec() {
        UDTCodec<Users.Name> codec = UDTCodec.create(session, "simplex", "fullname", Users.Name.class);
        assertNotNull(codec);
        assertNotNull(codec.getCqlType());
    }

    @Test
    public void test_UDTCodec_format_null_returnsNULL() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        assertEquals("NULL", codec.format(null));
    }

    @Test
    public void test_UDTCodec_parse_empty_returnsNull() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        assertNull(codec.parse(""));
        assertNull(codec.parse("NULL"));
    }

    @Test
    public void test_UDTCodec_serialize_null_returnsNull() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        // The protected serialize(T) returns null for null input.
        java.lang.reflect.Method m;
        try {
            // serialize(T) is protected abstract in the parent and overridden in anonymous subclass
            // Find it among the declared methods of the runtime class.
            m = null;
            for (java.lang.reflect.Method dm : codec.getClass().getDeclaredMethods()) {
                if (dm.getName().equals("serialize") && dm.getParameterCount() == 1) {
                    m = dm;
                    break;
                }
            }
            assertNotNull(m);
            m.setAccessible(true);
            Object result = m.invoke(codec, (Object) null);
            assertNull(result);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void test_UDTCodec_format_value_returnsJson() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        Users.Name n = new Users.Name();
        n.setFirstName("a");
        n.setLastName("b");
        String json = codec.format(n);
        assertNotNull(json);
        assertTrue(json.contains("a"));
        assertTrue(json.contains("b"));
    }

    @Test
    public void test_UDTCodec_parse_json_returnsObject() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        String json = "{\"firstName\":\"x\",\"lastName\":\"y\"}";
        Users.Name n = codec.parse(json);
        assertNotNull(n);
        assertEquals("x", n.getFirstName());
        assertEquals("y", n.getLastName());
    }

    @Test
    public void test_UDTCodec_getCqlType_isUDT() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        assertNotNull(codec.getCqlType());
        // The CQL type should equal the underlying UDT.
        assertEquals(lookupUDT("fullname").asCql(true, true), codec.getCqlType().asCql(true, true));
    }

    @Test
    public void test_UDTCodec_serializeBean_thenDeserialize_roundtrip() {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        Users.Name n = new Users.Name();
        n.setFirstName("first");
        n.setLastName("last");
        ByteBuffer encoded = codec.encode(n, ProtocolVersion.DEFAULT);
        assertNotNull(encoded);
        Users.Name back = codec.decode(encoded, ProtocolVersion.DEFAULT);
        assertNotNull(back);
        assertEquals("first", back.getFirstName());
        assertEquals("last", back.getLastName());
    }

    @Test
    public void test_UDTCodec_serialize_invalidType_throwsIAE() {
        // The UDTCodec produced by create() throws IAE if the class is neither Collection, Map, nor Bean.
        UDTCodec<Integer> codec = UDTCodec.create(lookupUDT("fullname"), Integer.class);
        assertThrows(IllegalArgumentException.class, () -> codec.encode(42, ProtocolVersion.DEFAULT));
    }

    @Test
    public void test_UDTCodec_serializeMap_roundtrip() {
        UDTCodec<Map> codec = UDTCodec.create(lookupUDT("fullname"), Map.class);
        // Cassandra unquoted identifiers are lowercased - use lower-case keys.
        Map<String, Object> m = new java.util.HashMap<>();
        m.put("firstname", "f");
        m.put("lastname", "l");
        ByteBuffer encoded = codec.encode(m, ProtocolVersion.DEFAULT);
        assertNotNull(encoded);
        Map back = codec.decode(encoded, ProtocolVersion.DEFAULT);
        assertNotNull(back);
        assertEquals("f", back.get("firstname"));
        assertEquals("l", back.get("lastname"));
    }

    @Test
    public void test_UDTCodec_serializeCollection_roundtrip() {
        UDTCodec<List> codec = UDTCodec.create(lookupUDT("fullname"), List.class);
        List<Object> values = N.asList("first", "last");
        ByteBuffer encoded = codec.encode(values, ProtocolVersion.DEFAULT);
        assertNotNull(encoded);
        // Decoding into a List returns a list of the values in field order.
        List back = codec.decode(encoded, ProtocolVersion.DEFAULT);
        assertNotNull(back);
        assertEquals(2, back.size());
    }

    @Test
    public void test_UDTCodec_deprecated_serializeDeserialize_roundtrip() throws Exception {
        UDTCodec<Users.Name> codec = UDTCodec.create(lookupUDT("fullname"), Users.Name.class);
        Users.Name n = new Users.Name();
        n.setFirstName("d1");
        n.setLastName("d2");
        // Use the deprecated public serialize(T, ProtocolVersion) overload.
        ByteBuffer bb = codec.serialize(n, ProtocolVersion.DEFAULT);
        assertNotNull(bb);
        Users.Name back = codec.deserialize(bb, ProtocolVersion.DEFAULT);
        assertNotNull(back);
        assertEquals("d1", back.getFirstName());
        assertEquals("d2", back.getLastName());
    }

    // ---------------------------------------------------------------------
    //  StatementSettings (v4) — extra fluent-setter and equals/hashCode coverage
    // ---------------------------------------------------------------------

    @Test
    public void test_StatementSettings_fluentSetters_returnSelf() {
        StatementSettings s = new StatementSettings();
        assertTrue(s == s.consistency(ConsistencyLevel.ONE));
        assertTrue(s == s.serialConsistency(ConsistencyLevel.SERIAL));
        assertTrue(s == s.fetchSize(10));
        assertTrue(s == s.timeout(Duration.ofSeconds(1)));
        assertTrue(s == s.traceQuery(Boolean.TRUE));
        assertEquals(ConsistencyLevel.ONE, s.consistency());
        assertEquals(10, s.fetchSize().intValue());
    }

    @Test
    public void test_StatementSettings_equalsAndHashCode() {
        Duration d = Duration.ofSeconds(5);
        StatementSettings a = new StatementSettings(ConsistencyLevel.ONE, ConsistencyLevel.SERIAL, 5, d, Boolean.TRUE);
        StatementSettings b = new StatementSettings(ConsistencyLevel.ONE, ConsistencyLevel.SERIAL, 5, d, Boolean.TRUE);
        StatementSettings c = new StatementSettings(ConsistencyLevel.TWO, ConsistencyLevel.SERIAL, 5, d, Boolean.TRUE);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }

    @Test
    public void test_StatementSettings_toString_containsValues() {
        StatementSettings s = new StatementSettings();
        s.fetchSize(123);
        String str = s.toString();
        assertNotNull(str);
        assertTrue(str.contains("123"));
    }

    // ---------------------------------------------------------------------
    //  Coverage gap fillers: batchInsert/Update with Map propsList, query, etc.
    // ---------------------------------------------------------------------

    @Test
    public void test_batchInsert_withMapPropsList() {
        // Exercises prepareBatchInsertStatement(Class, Collection<Map>, BatchType) - 0% coverage
        // Users has @Table("simplex.users") so the keyspace is resolved.
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        List<Map<String, Object>> propsList = N.asList(N.asMap("id", (Object) id1), N.asMap("id", (Object) id2));
        try {
            ResultSet rs = cassandraExecutor.batchInsert(Users.class, propsList, BatchType.LOGGED);
            assertNotNull(rs);
            assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.users WHERE id = ?", id1));
            assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.users WHERE id = ?", id2));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.users WHERE id = ?", id1);
            cassandraExecutor.execute("DELETE FROM simplex.users WHERE id = ?", id2);
        }
    }

    @Test
    public void test_batchUpdate_withMapPropsList() {
        // Exercises prepareBatchUpdateStatement(Class, Collection<Map>, BatchType) - 0% coverage
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.users (id, someDate) VALUES (?, ?)", id1, "d-before1");
        cassandraExecutor.execute("INSERT INTO simplex.users (id, someDate) VALUES (?, ?)", id2, "d-before2");
        try {
            List<Map<String, Object>> propsList = N.asList(N.asMap("id", (Object) id1, "someDate", (Object) "d-after1"),
                    N.asMap("id", (Object) id2, "someDate", (Object) "d-after2"));
            ResultSet rs = cassandraExecutor.batchUpdate(Users.class, propsList, BatchType.LOGGED);
            assertNotNull(rs);
            assertEquals("d-after1", cassandraExecutor.queryForSingleValue(String.class, "SELECT someDate FROM simplex.users WHERE id = ?", id1).orElse(null));
            assertEquals("d-after2", cassandraExecutor.queryForSingleValue(String.class, "SELECT someDate FROM simplex.users WHERE id = ?", id2).orElse(null));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.users WHERE id = ?", id1);
            cassandraExecutor.execute("DELETE FROM simplex.users WHERE id = ?", id2);
        }
    }

    @Test
    public void test_batchUpdate_withQueryAndParametersList() {
        // Exercises prepareBatchUpdateStatement(String, Collection, BatchType) - 0% coverage
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id1, "x");
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id2, "y");
        try {
            List<Object[]> params = N.asList(new Object[] { "u1", id1 }, new Object[] { "u2", id2 });
            ResultSet rs = cassandraExecutor.batchUpdate("UPDATE simplex.songs SET title = ? WHERE id = ?", params, BatchType.LOGGED);
            assertNotNull(rs);
            assertEquals("u1", cassandraExecutor.queryForSingleValue(String.class, "SELECT title FROM simplex.songs WHERE id = ?", id1).orElse(null));
            assertEquals("u2", cassandraExecutor.queryForSingleValue(String.class, "SELECT title FROM simplex.songs WHERE id = ?", id2).orElse(null));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id1);
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id2);
        }
    }

    @Test
    public void test_batchInsert_emptyEntities_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> cassandraExecutor.batchInsert(N.<Users> emptyList(), BatchType.LOGGED));
    }

    @Test
    public void test_batchInsert_emptyPropsList_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> cassandraExecutor.batchInsert(Users.class, N.<Map<String, Object>> emptyList(), BatchType.LOGGED));
    }

    @Test
    public void test_batchUpdate_emptyPropsList_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> cassandraExecutor.batchUpdate(Users.class, N.<Map<String, Object>> emptyList(), BatchType.LOGGED));
    }

    @Test
    public void test_batchUpdate_emptyQueryParams_throwsIAE() {
        assertThrows(IllegalArgumentException.class,
                () -> cassandraExecutor.batchUpdate("UPDATE simplex.songs SET title = ? WHERE id = ?", N.emptyList(), BatchType.LOGGED));
    }

    // ---------------------------------------------------------------------
    //  configStatement(Statement) coverage - executor created with StatementSettings
    // ---------------------------------------------------------------------

    @Test
    public void test_configStatement_appliedViaCustomSettings() {
        // Build settings exercising the consistency / fetchSize / timeout / tracing branches.
        StatementSettings settings = StatementSettings.builder()
                .consistency(ConsistencyLevel.ONE)
                .fetchSize(10)
                .timeout(Duration.ofSeconds(5))
                .traceQuery(Boolean.FALSE)
                .build();
        CassandraExecutor exec = new CassandraExecutor(cassandraExecutor.session(), settings);
        UUID id = UUID.randomUUID();
        exec.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "cfgT");
        try {
            // Run a select to ensure configStatement is exercised on a bound stmt path.
            assertTrue(exec.exists("SELECT * FROM simplex.songs WHERE id = ?", id));
            // Exercise configStatement on a BatchStatement via batchUpdate(String, ...).
            UUID b1 = UUID.randomUUID();
            UUID b2 = UUID.randomUUID();
            exec.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", b1, "b1");
            exec.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", b2, "b2");
            List<Object[]> params = N.asList(new Object[] { "b1-new", b1 }, new Object[] { "b2-new", b2 });
            assertNotNull(exec.batchUpdate("UPDATE simplex.songs SET title = ? WHERE id = ?", params, BatchType.LOGGED));
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", b1);
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", b2);
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_configStatement_withSerialConsistency() {
        // Cover the serialConsistency branch of configStatement.
        StatementSettings settings = StatementSettings.builder().serialConsistency(ConsistencyLevel.LOCAL_SERIAL).build();
        CassandraExecutor exec = new CassandraExecutor(cassandraExecutor.session(), settings);
        // Plain SELECT - just confirms the configured stmt executes.
        ResultSet rs = exec.execute("SELECT id FROM simplex.songs LIMIT 1");
        assertNotNull(rs);
    }

    // ---------------------------------------------------------------------
    //  readRow / createRowMapper coverage - exercise object[], collection,
    //  and single-column-with-conversion lambda branches.
    // ---------------------------------------------------------------------

    @Test
    public void test_list_withObjectArrayClass() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "oa");
        try {
            // Use list(Object[].class, ...) which exercises the isObjectArray branch in createRowMapper/readRow.
            List<Object[]> rows = cassandraExecutor.list(Object[].class, "SELECT id, title FROM simplex.songs WHERE id = ?", id);
            assertEquals(1, rows.size());
            assertEquals(2, rows.get(0).length);
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_list_withListClass() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "lc");
        try {
            // Use list(List.class, ...) which exercises the isCollection branch.
            List<List> rows = (List) cassandraExecutor.list(List.class, "SELECT id, title FROM simplex.songs WHERE id = ?", id);
            assertEquals(1, rows.size());
            assertEquals(2, rows.get(0).size());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_stream_withMapClass() {
        // Stream with Map.class exercises the isMap branch in createRowMapper.
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "stM");
        try {
            long count = cassandraExecutor.stream(Map.class, "SELECT id, title FROM simplex.songs WHERE id = ?", id).count();
            assertEquals(1L, count);
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_stream_withObjectArrayClass() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "stOA");
        try {
            long count = cassandraExecutor.stream(Object[].class, "SELECT id, title FROM simplex.songs WHERE id = ?", id).count();
            assertEquals(1L, count);
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_stream_withStringClass_singleColumn() {
        // Exercises the single-column-conversion lambda branch (rowClass=String, value=UUID -> N.convert).
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "stC");
        try {
            List<String> titles = cassandraExecutor.stream(String.class, "SELECT title FROM simplex.songs WHERE id = ?", id).toList();
            assertEquals(1, titles.size());
            assertEquals("stC", titles.get(0));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_list_singleColumn_typeConversion() {
        // Exercises N.convert path in single-column row mapper: UUID -> String conversion.
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "convT");
        try {
            // Force a conversion: the column is a UUID, but request String back.
            List<String> ids = cassandraExecutor.list(String.class, "SELECT id FROM simplex.songs WHERE id = ?", id);
            assertEquals(1, ids.size());
            assertEquals(id.toString(), ids.get(0));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    // Coverage additions

    @Test
    public void test_execute_withBeanParam() {
        Users user = createUser();
        String sql = NSC.insertInto(Users.class).build().query();
        cassandraExecutor.execute(sql, user);
        try {
            assertTrue(cassandraExecutor.exists("SELECT * FROM simplex.users WHERE id = ?", user.getId()));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.users WHERE id = ?", user.getId());
        }
    }

    @Test
    public void test_execute_withBeanParam_missingPropertyThrowsIAE() {
        Song s = new Song();
        s.setId(UUID.randomUUID());
        s.setTitle("t");
        assertThrows(IllegalArgumentException.class, () -> cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = :nonExistentBeanProp", s));
    }

    @Test
    public void test_list_singleColumn_noConversionNeeded() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "noConv");
        try {
            List<UUID> ids = cassandraExecutor.list(UUID.class, "SELECT id FROM simplex.songs WHERE id = ?", id);
            assertEquals(1, ids.size());
            assertEquals(id, ids.get(0));
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_toList_mapClass_columnsAsMap() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "asMap");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT id, title FROM simplex.songs WHERE id = ?", id);
            @SuppressWarnings("rawtypes")
            List<Map> rows = CassandraExecutor.toList(rs, Map.class);
            assertEquals(1, rows.size());
            assertEquals(2, rows.get(0).size());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_execute_collectionArgWrappedInArray() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "collArg");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT * FROM simplex.songs WHERE id = ?", N.asList(id));
            assertNotNull(rs.one());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
    }

    @Test
    public void test_extractData_listClassConversionForUuid() {
        UUID id = UUID.randomUUID();
        cassandraExecutor.execute("INSERT INTO simplex.songs (id, title) VALUES (?, ?)", id, "convForExtract");
        try {
            ResultSet rs = cassandraExecutor.execute("SELECT id FROM simplex.songs WHERE id = ?", id);
            Dataset ds = CassandraExecutor.extractData(rs, String.class);
            assertNotNull(ds);
            assertEquals(1, ds.size());
        } finally {
            cassandraExecutor.execute("DELETE FROM simplex.songs WHERE id = ?", id);
        }
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
        user.setLastUpdateTime(LocalTime.now());
        user.setCreatedTime(Dates.currentTimestamp());

        user.setBytes(N.asList((byte) 1, (byte) 2, (byte) 3));
        user.setShorts(N.asSet((short) 1, (short) 2, (short) 3));

        return user;
    }
}
