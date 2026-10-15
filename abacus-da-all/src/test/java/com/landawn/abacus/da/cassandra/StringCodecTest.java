package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.landawn.abacus.da.TestBase;
import com.landawn.abacus.da.entity.Song;

public class StringCodecTest extends TestBase {

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
    public void encode_null_returnsNullBuffer() {
        assertNull(newStringCodec(String.class).encode(null, ProtocolVersion.DEFAULT));
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

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = { "", "NULL" })
    void parse_emptyOrNullLiteral_returnsNull(final String literal) {
        assertNull(newStringCodec(Song.class).parse(literal));
    }
}
