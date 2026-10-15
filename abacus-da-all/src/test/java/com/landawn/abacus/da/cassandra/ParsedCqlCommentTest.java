package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ParsedCqlCommentTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("queries")
    void commentsDoNotExposeMarkersInsideQuotedValues(final String label, final String input, final String expected) {
        final ParsedCql parsed = ParsedCql.parse(input);
        assertEquals(expected, parsed.parameterizedCql());
        assertEquals(1, parsed.parameterCount());
        assertEquals(Map.of(0, "id"), parsed.namedParameters());
    }

    static Stream<Arguments> queries() {
        final List<String> prefixes = List.of("", "// leading :ignored\r\n", "-- leading // :ignored\n", "/* leading // :ignored */\n");
        final List<String> literals = List.of("'O''Brien // :ignored'", "$$ // text :ignored ? #{ignored} $$", "'http://host/path'", "['http://host', 'x''y']");
        return prefixes.stream()
                .flatMap(prefix -> literals.stream()
                        .map(literal -> Arguments.of(prefix.replace("\r", "\\r").replace("\n", "\\n") + literal,
                                prefix + "UPDATE items SET value = " + literal + " WHERE id = :id // trailing :ignored",
                                "UPDATE items SET value = " + literal + " WHERE id = ?")));
    }
}
