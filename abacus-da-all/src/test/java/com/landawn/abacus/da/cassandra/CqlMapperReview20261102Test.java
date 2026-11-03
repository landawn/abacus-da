package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * Review 2026-11-02 (slice s15): pins documented {@link CqlMapper} XML behaviors and a seeded marker-count invariant of
 * {@link ParsedCql}. No behavior change was made in this round; these guard the documented contracts.
 */
class CqlMapperReview20261102Test {

    private static CqlMapper load(final String xml) {
        return CqlMapper.loadFrom(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    private static String save(final CqlMapper mapper) {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        mapper.saveTo(os);
        return os.toString(StandardCharsets.UTF_8);
    }

    @Test
    void nullAttributeValueIsSavedAsEmptyValue() {
        final CqlMapper mapper = new CqlMapper();
        final Map<String, String> attrs = new HashMap<>();
        attrs.put("timeout", null);
        mapper.add("a", "SELECT 1 FROM t", attrs);

        assertTrue(mapper.getAttributes("a").containsKey("timeout"));
        assertNull(mapper.getAttributes("a").get("timeout"));

        final CqlMapper reloaded = load(save(mapper));
        assertEquals("", reloaded.getAttributes("a").get("timeout"));
        assertFalse(reloaded.equals(mapper));
    }

    @Test
    void onlyDirectCqlChildrenAreReadAndCommentsAreDropped() {
        final CqlMapper mapper = load("<cqlMapper><group><cql id=\"deep\">SELECT 2 FROM t</cql></group><other/>"
                + "<cql id=\"a\">SELECT * <!-- note --> FROM <b>t</b> WHERE a = :a</cql></cqlMapper>");

        assertEquals(List.of("a"), List.copyOf(mapper.ids()));
        assertEquals("SELECT *  FROM t WHERE a = :a", mapper.get("a").originalCql());
        assertEquals("SELECT * FROM t WHERE a = ?", mapper.get("a").parameterizedCql());
    }

    @Test
    void idsSnapshotAfterRemove() {
        final CqlMapper mapper = new CqlMapper();
        mapper.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
        mapper.add("findAll", "SELECT * FROM users", null);
        assertEquals("[findUserById, findAll]", mapper.ids().toString());

        mapper.remove("findAll");
        assertEquals("[findUserById]", mapper.ids().toString());
    }

    // ---- ParsedCql: seeded generator over quoted/dollar-quoted literals, all three comment styles, list subscripts,
    // tuples, map/UDT literals (spaced and unspaced separators) and all three marker styles. Invariant: every generated
    // marker is counted once, the rewritten CQL holds exactly that many '?' outside literals/comments and no native
    // ':name' marker, and the name map has one entry per named marker.

    private static final char NL = '\n';

    private static final String[] LITERALS = { "'s'", "'it''s :z ?'", "\"Col:x\"", "$$ :d ? ' -- $$", "'\\'", "'a\\'", "1", "-1", "0x0A", "'[x]'", "'{k}'",
            "'#'", "$$[ ]$$" };

    private static final String[] COMMENTS = { "", " -- c :q ?" + NL, " /* :q ? ' */ ", " // c :q '" + NL, " ", NL + "  " };

    private static final String[] OPERATORS = { " = ", "=", " > ", ">=", " != ", " IN ", " CONTAINS " };

    private static String marker(final int style, final Random random) {
        return switch (style) {
            case 0 -> "?";
            case 1 -> new String[] { ":a", ":bb", ":c1", ":\"Q\"" }[random.nextInt(4)];
            default -> new String[] { "#{x}", "#{ y , jdbcType=INT }", "#{z.w}" }[random.nextInt(3)];
        };
    }

    /** Counts {@code ?} and native {@code :name} markers outside quoted text, dollar-quoted text and comments (CQL lexing). */
    private static int[] countMarkers(final String cql) {
        int questionMarks = 0;
        int named = 0;

        for (int i = 0, n = cql.length(); i < n; i++) {
            final char c = cql.charAt(i);

            if (c == '\'' || c == '"') {
                for (i++; i < n; i++) {
                    if (cql.charAt(i) == c) {
                        if (i + 1 < n && cql.charAt(i + 1) == c) {
                            i++;
                        } else {
                            break;
                        }
                    }
                }
            } else if (c == '$' && i + 1 < n && cql.charAt(i + 1) == '$') {
                final int end = cql.indexOf("$$", i + 2);
                i = end < 0 ? n : end + 1;
            } else if ((c == '-' || c == '/') && i + 1 < n && cql.charAt(i + 1) == c) {
                while (i < n && cql.charAt(i) != NL) {
                    i++;
                }
            } else if (c == '/' && i + 1 < n && cql.charAt(i + 1) == '*') {
                final int end = cql.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 1;
            } else if (c == '?') {
                questionMarks++;
            } else if (c == ':' && i + 1 < n && Character.isLetter(cql.charAt(i + 1))
                    && (i == 0 || !Character.isLetterOrDigit(cql.charAt(i - 1)) && ":'\"".indexOf(cql.charAt(i - 1)) < 0)) {
                named++;
            }
        }

        return new int[] { questionMarks, named };
    }

    @Test
    void seededMarkerCountInvariant() {
        final Random random = new Random(20261102L);

        for (int t = 0; t < 3000; t++) {
            final int style = random.nextInt(3);
            final StringBuilder sb = new StringBuilder(random.nextBoolean() ? "SELECT * FROM t WHERE " : "UPDATE t SET v = 1 WHERE ");
            int expected = 0;

            for (int k = 0, terms = 1 + random.nextInt(5); k < terms; k++) {
                if (k > 0) {
                    sb.append(random.nextBoolean() ? " AND " : " AND" + COMMENTS[random.nextInt(COMMENTS.length)]);
                }

                sb.append('c').append(k).append(OPERATORS[random.nextInt(OPERATORS.length)]);

                switch (random.nextInt(5)) {
                    case 0 -> sb.append(LITERALS[random.nextInt(LITERALS.length)]);
                    case 1 -> {
                        sb.append('(').append(marker(style, random)).append(", ").append(LITERALS[random.nextInt(LITERALS.length)]).append(')');
                        expected++;
                    }
                    case 2 -> {
                        sb.append('[').append(marker(style, random)).append(']');
                        expected++;
                    }
                    case 3 -> {
                        final boolean map = random.nextBoolean();
                        final String separator = random.nextBoolean() ? ": " : ":";
                        final String comma = random.nextBoolean() ? ", " : ",";
                        sb.append('{');

                        for (int f = 0, fields = 1 + random.nextInt(3); f < fields; f++) {
                            if (f > 0) {
                                sb.append(comma);
                            }

                            sb.append(map ? new String[] { "'k'", "'a:b'", "1", "'{'", "'}'", "$$k:$$" }[random.nextInt(6)]
                                    : new String[] { "f", "g1", "\"F\"" }[random.nextInt(3)]).append(separator);

                            switch (random.nextInt(5)) {
                                case 0 -> sb.append(LITERALS[random.nextInt(LITERALS.length)]);
                                case 1 -> {
                                    sb.append('(').append(marker(style, random)).append(", ").append(marker(style, random)).append(')');
                                    expected += 2;
                                }
                                case 2 -> {
                                    sb.append('[').append(marker(style, random)).append(']');
                                    expected++;
                                }
                                case 3 -> {
                                    sb.append("1 + ").append(marker(style, random));
                                    expected++;
                                }
                                default -> {
                                    sb.append(marker(style, random));
                                    expected++;
                                }
                            }
                        }

                        sb.append('}');
                    }
                    default -> {
                        sb.append(marker(style, random));
                        expected++;
                    }
                }

                sb.append(COMMENTS[random.nextInt(COMMENTS.length)]);
            }

            if (random.nextBoolean()) {
                sb.append(';');
            }

            final String cql = sb.toString();
            final ParsedCql parsed = ParsedCql.parse(cql);
            final int[] counts = countMarkers(parsed.parameterizedCql());

            assertEquals(expected, parsed.parameterCount(), cql);
            assertEquals(expected, counts[0], () -> cql + " -> " + parsed.parameterizedCql());
            assertEquals(0, counts[1], () -> cql + " -> " + parsed.parameterizedCql());
            assertEquals(style == 0 ? 0 : expected, parsed.namedParameters().size(), cql);
        }
    }
}
