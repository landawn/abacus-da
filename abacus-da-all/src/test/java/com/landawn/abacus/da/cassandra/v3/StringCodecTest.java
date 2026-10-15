package com.landawn.abacus.da.cassandra.v3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Song;

public class StringCodecTest extends TestBase {

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

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = { "", "NULL" })
    void parse_emptyOrNullLiteral_returnsNull(final String literal) {
        assertNull(newStringCodec(Song.class).parse(literal));
    }
}
