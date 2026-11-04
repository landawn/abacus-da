package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.landawn.abacus.annotation.AccessFieldByMethod;
import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.parser.ParserUtil;
import com.landawn.abacus.parser.ParserUtil.BeanInfo;
import com.landawn.abacus.parser.ParserUtil.PropInfo;
import com.landawn.abacus.query.Filters;
import com.landawn.abacus.query.condition.Condition;
import com.landawn.abacus.query.condition.Criteria;

public class CassandraExecutorBaseReview20261103Test extends CassandraExecutorBaseTestSupport {

    // Confirms the 2026-11-02 re-review's open finding is closed in the shared helpers the v3 and v4 row readers (and UDT
    // decoders) use: a NULL column added to a collection row that rejects null elements is an IllegalArgumentException naming the
    // column and the collection class (cause: the collection's own exception), like a map row; a null-accepting row class keeps
    // the NULL. An ArrayBlockingQueue row is created with room for every column (at least one).
    @Test
    public void test_nullColumnIntoNullRejectingRowCollectionNamesTheColumn() {
        assertAll(() -> assertRejectsNull(ArrayBlockingQueue.class, ArrayBlockingQueue.class), () -> assertRejectsNull(ArrayDeque.class, ArrayDeque.class),
                () -> assertRejectsNull(ConcurrentLinkedQueue.class, ConcurrentLinkedQueue.class), () -> assertRejectsNull(TreeSet.class, TreeSet.class),
                () -> assertRejectsNull(PriorityQueue.class, PriorityQueue.class), () -> assertRejectsNull(BlockingQueue.class, LinkedBlockingQueue.class),
                () -> assertKeepsNull(ArrayList.class, ArrayList.class), () -> assertKeepsNull(LinkedList.class, LinkedList.class),
                () -> assertKeepsNull(Queue.class, LinkedList.class), () -> assertKeepsNull(Deque.class, LinkedList.class),
                () -> assertEquals(3, ((ArrayBlockingQueue<?>) CassandraExecutorBase.newRowCollection(ArrayBlockingQueue.class, 3)).remainingCapacity()),
                () -> assertEquals(1, ((ArrayBlockingQueue<?>) CassandraExecutorBase.newRowCollection(ArrayBlockingQueue.class, 0)).remainingCapacity()),
                () -> {
                    // A value a sorted row cannot compare with the earlier ones: the ClassCastException is wrapped the same way.
                    final Collection<Object> sorted = CassandraExecutorBase.newRowCollection(TreeSet.class, 2);
                    CassandraExecutorBase.addRowValue(sorted, "Field", "a", "x");
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> CassandraExecutorBase.addRowValue(sorted, "Field", "b", 5));
                    assertEquals("Field (b, 5) is not accepted by the row collection java.util.TreeSet", e.getMessage());
                    assertInstanceOf(ClassCastException.class, e.getCause());
                });
    }

    // Cross-cut (s22): every null-rejecting or sorted row container, map or collection, is covered by the shared helpers — a NULL
    // value into a ConcurrentSkipListMap / ConcurrentHashMap / Hashtable map row or a ConcurrentSkipListSet collection row, and a
    // value a sorted set cannot compare, is an IllegalArgumentException naming the column and the container (cause kept); a
    // null-accepting container (TreeMap with String keys, HashMap, LinkedHashMap) keeps the NULL.
    @Test
    public void test_everyNullRejectingRowContainerNamesTheColumn() {
        assertAll(() -> assertMapRejectsNull(new java.util.concurrent.ConcurrentSkipListMap<>()),
                () -> assertMapRejectsNull(new java.util.concurrent.ConcurrentHashMap<>()), () -> assertMapRejectsNull(new java.util.Hashtable<>()),
                () -> assertMapKeepsNull(new java.util.TreeMap<>()), () -> assertMapKeepsNull(new java.util.HashMap<>()),
                () -> assertMapKeepsNull(new java.util.LinkedHashMap<>()),
                () -> assertRejectsNull(java.util.concurrent.ConcurrentSkipListSet.class, java.util.concurrent.ConcurrentSkipListSet.class), () -> {
                    final Collection<Object> sorted = CassandraExecutorBase.newRowCollection(java.util.concurrent.ConcurrentSkipListSet.class, 2);
                    CassandraExecutorBase.addRowValue(sorted, "Column", "a", "x");
                    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> CassandraExecutorBase.addRowValue(sorted, "Column", "b", 5));
                    assertEquals("Column (b, 5) is not accepted by the row collection java.util.concurrent.ConcurrentSkipListSet", e.getMessage());
                    assertInstanceOf(ClassCastException.class, e.getCause());
                });
    }

    private static void assertMapRejectsNull(final java.util.Map<String, Object> map) {
        CassandraExecutorBase.putRowValue(map, "Column", "a", "x");

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.putRowValue(map, "Column", "b", null));

        assertEquals("Column (b, null) is not accepted by the row map " + map.getClass().getName(), e.getMessage());
        assertInstanceOf(NullPointerException.class, e.getCause());
        assertEquals(java.util.Map.of("a", "x"), map);
    }

    private static void assertMapKeepsNull(final java.util.Map<String, Object> map) {
        CassandraExecutorBase.putRowValue(map, "Column", "a", "x");
        CassandraExecutorBase.putRowValue(map, "Column", "b", null);

        assertAll(() -> assertEquals(2, map.size()), () -> assertTrue(map.containsKey("b")), () -> assertNull(map.get("b")));
    }

    // HEAD: a null collection failed inside the catch block while building the message, with a NullPointerException about
    // Collection.getClass() that hid the original one (putRowValue rethrows the original for a null map). Now the same as putRowValue.
    @Test
    public void test_addRowValueRethrowsTheNullPointerExceptionOfANullCollection() {
        final NullPointerException e = assertThrows(NullPointerException.class, () -> CassandraExecutorBase.addRowValue(null, "Column", "a", "x"));

        assertAll(() -> assertTrue(String.valueOf(e.getMessage()).contains("add("), String.valueOf(e.getMessage())),
                () -> assertFalse(String.valueOf(e.getMessage()).contains("getClass"), String.valueOf(e.getMessage())), () -> assertNull(e.getCause()));
    }

    // Pins the dotted-path helper's documented edge cases, each the same as the path setter it replaces: a null intermediate of a
    // class without a no-argument constructor (a record) fails with IllegalArgumentException; a path through a collection is left
    // to the path setter before anything is created; an existing intermediate that reads back stably is written in place. (A
    // getter-only bean property is not a BeanInfo property, so a read-only intermediate never reaches the helper: no chain.)
    @Test
    public void test_dottedPathHelperEdgeCasesMatchThePathSetter() {
        final BeanInfo pointInfo = ParserUtil.getBeanInfo(PointOwner.class);
        final BeanInfo bookInfo = ParserUtil.getBeanInfo(AddressBookOwner.class);
        final AddressBookOwner bookOwner = new AddressBookOwner();
        final BeanInfo homeInfo = ParserUtil.getBeanInfo(HomeOwner.class);
        final HomeOwner homeOwner = new HomeOwner();
        final Address home = new Address();
        homeOwner.setHome(home);

        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> CassandraExecutorBase.setDottedPathValue(new PointOwner(), pointInfo.getPropInfoChain("point.x"), 1, null)),
                () -> assertThrows(IllegalArgumentException.class, () -> pointInfo.setPropValue(new PointOwner(), "point.x", 1, true)),
                () -> assertFalse(CassandraExecutorBase.setDottedPathValue(bookOwner, bookInfo.getPropInfoChain("addresses.city"), "Paris", null)),
                () -> assertNull(bookOwner.getAddresses()),
                () -> assertTrue(CassandraExecutorBase.setDottedPathValue(homeOwner, homeInfo.getPropInfoChain("home.city"), "Paris", null)),
                () -> assertSame(home, homeOwner.getHome()), () -> assertEquals("Paris", home.getCity()), () -> assertEquals(List.of(), getterOnlyChain()));
    }

    private static List<PropInfo> getterOnlyChain() {
        return ParserUtil.getBeanInfo(GetterOnlyAddressOwner.class).getPropInfoChain("address.city");
    }

    // Pins the per-method @throws IllegalStateException added to the Condition-based methods that append their own LIMIT (see the
    // class documentation's "Row Limits"): each one rejects a condition that carries a LIMIT before any CQL runs.
    @Test
    public void test_everyLimitAppendingConditionMethodRejectsAConditionWithALimit() {
        final Condition limited = Criteria.builder().where(Filters.eq("accountId", 7L)).limit(5).build();
        final RecordingExecutor recording = new RecordingExecutor();
        final Class<LimitAccount> cls = LimitAccount.class;

        final List<Executable> calls = Arrays.asList(() -> recording.findFirst(cls, List.of("name"), limited), () -> recording.exists(cls, limited),
                () -> recording.queryForBoolean(cls, "name", limited), () -> recording.queryForChar(cls, "name", limited),
                () -> recording.queryForByte(cls, "name", limited), () -> recording.queryForShort(cls, "name", limited),
                () -> recording.queryForInt(cls, "name", limited), () -> recording.queryForLong(cls, "name", limited),
                () -> recording.queryForFloat(cls, "name", limited), () -> recording.queryForDouble(cls, "name", limited),
                () -> recording.queryForString(cls, "name", limited), () -> recording.queryForDate(cls, "name", limited),
                () -> recording.queryForDate(cls, java.sql.Timestamp.class, "name", limited),
                () -> recording.queryForSingleValue(cls, String.class, "name", limited),
                () -> recording.queryForSingleNonNull(cls, String.class, "name", limited));

        assertAll(calls.stream().map(call -> (Executable) () -> assertThrows(IllegalStateException.class, call)));
        assertEquals(List.of(), recording.recorded);
    }

    // HEAD: registerKeys resolved a key only through Beans.getPropGetter, so a key property backed by a public field alone (which
    // BeanInfo, @Id, insert/update and entityToCondition all read) was rejected with "No readable property found". Now a name
    // the getter lookup misses falls back to the bean's own property of that name when that property has no getter; a getter
    // bean, and a name that is no property, are treated as before.
    @Test
    @SuppressWarnings("deprecation")
    public void test_registerKeysAcceptsAPublicFieldKeyProperty() {
        try {
            CassandraExecutorBase.registerKeys(FieldKeyEntity.class, List.of("tenant_id", "userId"));
            CassandraExecutorBase.registerKeys(MixedKeyEntity.class, List.of("userId", "tenantId"));

            final FieldKeyEntity entity = new FieldKeyEntity();
            entity.tenantId = "t1";
            entity.userId = "u1";

            assertAll(() -> assertEquals(List.of("tenantId", "userId"), CassandraExecutorBase.getKeyNames(FieldKeyEntity.class)),
                    () -> assertEquals(List.of("userId", "tenantId"), CassandraExecutorBase.getKeyNames(MixedKeyEntity.class)),
                    () -> assertEquals(
                            "SELECT user_id AS \"userId\", tenant_id AS \"tenantId\", name AS \"name\" FROM field_key_rows"
                                    + " WHERE tenant_id = :tenantId AND user_id = :userId",
                            executor.exposedPrepareQuery(FieldKeyEntity.class, null, CassandraExecutorBase.entityToCondition(entity), 0).query()),
                    () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.registerKeys(FieldKeyEntity.class, List.of("nope"))),
                    // A getter bean keeps the getter-only lookup: a @Column name is still not a key name it accepts.
                    () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.registerKeys(ColumnKeyEntity.class, List.of("acct"))),
                    () -> assertEquals(List.of("tenantId", "userId"), CassandraExecutorBase.getKeyNames(FieldKeyEntity.class)));
        } finally {
            CassandraExecutorBase.entityKeyNamesMap.remove(FieldKeyEntity.class);
            CassandraExecutorBase.entityKeyNamesMap.remove(MixedKeyEntity.class);
            CassandraExecutorBase.entityKeyNamesMap.remove(ColumnKeyEntity.class);
        }
    }

    /**
     * RED on HEAD: registerKeys asked the getter lookup first, which also accepts is/has-prefixed spellings, so the key name
     * {@code isActive} of a field-only property registered the getter-backed {@code active} (getActive()), and {@code haschildren}
     * (the lower-cased column of the field-only {@code hasChildren}) registered {@code children}. A field-only property the bean's
     * properties resolve the name to is now preferred; exact getter names, and a getter-backed property reached only through an
     * {@code @JsonXmlField} alias (still rejected), are unchanged.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void test_registerKeysPrefersAFieldOnlyPropertyOverAPrefixedGetter() {
        try {
            assertAll(() -> {
                CassandraExecutorBase.registerKeys(PrefixShadowKeyEntity.class, List.of("isActive", "haschildren"));
                assertEquals(List.of("isActive", "hasChildren"), CassandraExecutorBase.getKeyNames(PrefixShadowKeyEntity.class));
            }, () -> {
                CassandraExecutorBase.registerKeys(PrefixShadowKeyEntity.class, List.of("active", "children"));
                assertEquals(List.of("active", "children"), CassandraExecutorBase.getKeyNames(PrefixShadowKeyEntity.class));
            }, () -> {
                // Without a field-only property of that name, a prefixed spelling still resolves to the getter-backed one, as before.
                CassandraExecutorBase.registerKeys(MixedKeyEntity.class, List.of("getUserId"));
                assertEquals(List.of("userId"), CassandraExecutorBase.getKeyNames(MixedKeyEntity.class));
            }, () -> {
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> CassandraExecutorBase.registerKeys(AliasKeyEntity.class, List.of("nm")));
                assertTrue(e.getMessage().startsWith("No readable property found for key name: nm"), e.getMessage());
            });
        } finally {
            CassandraExecutorBase.entityKeyNamesMap.remove(PrefixShadowKeyEntity.class);
            CassandraExecutorBase.entityKeyNamesMap.remove(MixedKeyEntity.class);
            CassandraExecutorBase.entityKeyNamesMap.remove(AliasKeyEntity.class);
        }
    }

    /**
     * RED before this fix (verifier follow-up): {@code URL}/{@code getURL()} and {@code ID}/{@code getID()} counted as field-only
     * (the getter's derived name differs from the property name by case), so their {@code @JsonXmlField} name or alias was accepted
     * as a key name; it is rejected again, as on HEAD. A field-only {@code ID} beside a getter-backed {@code id} is registered by its
     * exact name, and {@code id} still registers the getter-backed property.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void test_registerKeysTreatsAcronymGetterPropertiesAsGetterBacked() {
        try {
            assertAll(() -> {
                final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> CassandraExecutorBase.registerKeys(AcronymKeyEntity.class, List.of("theId")));
                assertTrue(e.getMessage().startsWith("No readable property found for key name: theId"), e.getMessage());
            }, () -> assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.registerKeys(AcronymKeyEntity.class, List.of("link"))), () -> {
                // Unchanged from HEAD: a getter-resolved key is registered under the getter's derived name.
                CassandraExecutorBase.registerKeys(AcronymKeyEntity.class, List.of("URL", "ID"));
                assertEquals(List.of("url", "id"), CassandraExecutorBase.getKeyNames(AcronymKeyEntity.class));
            }, () -> {
                CassandraExecutorBase.registerKeys(CaseSiblingKeyEntity.class, List.of("ID"));
                assertEquals(List.of("ID"), CassandraExecutorBase.getKeyNames(CaseSiblingKeyEntity.class));
                CassandraExecutorBase.registerKeys(CaseSiblingKeyEntity.class, List.of("id"));
                assertEquals(List.of("id"), CassandraExecutorBase.getKeyNames(CaseSiblingKeyEntity.class));
            });
        } finally {
            CassandraExecutorBase.entityKeyNamesMap.remove(AcronymKeyEntity.class);
            CassandraExecutorBase.entityKeyNamesMap.remove(CaseSiblingKeyEntity.class);
        }
    }

    /**
     * RED before this fix: the guard compared the name with the getter's derived name, and {@code getURL()} (the getter of the
     * property {@code URL}) derives {@code url}, so for the name {@code url} it kept the getter although the bean's exact property
     * {@code url} is a field-only one. It now compares with the getter's own property name; {@code URL} still keeps its getter, and
     * registerKeys registers {@code url} for either spelling (unchanged: a getter-resolved key keeps the getter's derived name).
     */
    @Test
    @SuppressWarnings("deprecation")
    public void test_exactNameFieldOnlyPropertyWinsOverAnAcronymGetterOfTheSameDerivedName() throws Exception {
        final java.lang.reflect.Method getURL = AcronymSiblingKeyEntity.class.getMethod("getURL");

        try {
            assertAll(() -> {
                final PropInfo propInfo = CassandraExecutorBase.fieldOnlyPropInfoShadowedByGetterAlias(AcronymSiblingKeyEntity.class, "url", getURL);
                assertEquals("url", propInfo == null ? null : propInfo.name);
                assertEquals(String.class, propInfo.clazz);
            }, () -> assertNull(CassandraExecutorBase.fieldOnlyPropInfoShadowedByGetterAlias(AcronymSiblingKeyEntity.class, "URL", getURL)),
                    () -> assertNull(CassandraExecutorBase.fieldOnlyPropInfoShadowedByGetterAlias(AcronymSiblingKeyEntity.class, "Url", getURL)), () -> {
                        CassandraExecutorBase.registerKeys(AcronymSiblingKeyEntity.class, List.of("url"));
                        assertEquals(List.of("url"), CassandraExecutorBase.getKeyNames(AcronymSiblingKeyEntity.class));
                    });
        } finally {
            CassandraExecutorBase.entityKeyNamesMap.remove(AcronymSiblingKeyEntity.class);
        }
    }

    /** A field-only {@code url} beside the getter-backed {@code URL} whose getter {@code getURL()} derives the name {@code url}. */
    public static class AcronymSiblingKeyEntity {
        public String url;
        private Integer URL;

        public Integer getURL() {
            return URL;
        }

        public void setURL(final Integer url) {
            URL = url;
        }
    }

    public static class AcronymKeyEntity {
        @com.landawn.abacus.annotation.JsonXmlField(name = "theId")
        private String ID;
        @com.landawn.abacus.annotation.JsonXmlField(aliases = "link")
        private String URL;

        public String getID() {
            return ID;
        }

        public void setID(final String id) {
            ID = id;
        }

        public String getURL() {
            return URL;
        }

        public void setURL(final String url) {
            URL = url;
        }
    }

    public static class CaseSiblingKeyEntity {
        public String ID;
        private String id;

        public String getId() {
            return id;
        }

        public void setId(final String id) {
            this.id = id;
        }
    }

    /** Getter-backed {@code active}/{@code children} beside field-only {@code isActive}/{@code hasChildren}. */
    public static class PrefixShadowKeyEntity {
        private String active;
        private Integer children;
        public String isActive;
        public String hasChildren;

        public String getActive() {
            return active;
        }

        public void setActive(final String active) {
            this.active = active;
        }

        public Integer getChildren() {
            return children;
        }

        public void setChildren(final Integer children) {
            this.children = children;
        }
    }

    public static class AliasKeyEntity {
        @com.landawn.abacus.annotation.JsonXmlField(aliases = "nm")
        private String name;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    @Table("field_key_rows")
    public static class FieldKeyEntity {
        public String userId;
        public String tenantId;
        public String name;
    }

    public static class MixedKeyEntity {
        public String tenantId;
        private String userId;

        public String getUserId() {
            return userId;
        }

        public void setUserId(final String userId) {
            this.userId = userId;
        }
    }

    public static class ColumnKeyEntity {
        @com.landawn.abacus.annotation.Column("acct")
        private Long accountId;

        public Long getAccountId() {
            return accountId;
        }

        public void setAccountId(final Long accountId) {
            this.accountId = accountId;
        }
    }

    private static void assertRejectsNull(final Class<?> rowClass, final Class<?> createdClass) {
        final Collection<Object> row = CassandraExecutorBase.newRowCollection(rowClass, 2);
        assertEquals(createdClass, row.getClass());
        CassandraExecutorBase.addRowValue(row, "Column", "a", "x");

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CassandraExecutorBase.addRowValue(row, "Column", "b", null));

        assertEquals("Column (b, null) is not accepted by the row collection " + createdClass.getName(), e.getMessage());
        assertInstanceOf(NullPointerException.class, e.getCause());
        assertEquals(List.of("x"), new ArrayList<>(row));
    }

    private static void assertKeepsNull(final Class<?> rowClass, final Class<?> createdClass) {
        final Collection<Object> row = CassandraExecutorBase.newRowCollection(rowClass, 2);
        assertEquals(createdClass, row.getClass());
        CassandraExecutorBase.addRowValue(row, "Column", "a", "x");
        CassandraExecutorBase.addRowValue(row, "Column", "b", null);

        assertEquals(Arrays.asList("x", null), new ArrayList<>(row));
    }

    @Table("limit_accounts")
    public static class LimitAccount {
        @Id
        private Long accountId;
        private String name;

        public Long getAccountId() {
            return accountId;
        }

        public void setAccountId(final Long accountId) {
            this.accountId = accountId;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class Address {
        private String city;

        public String getCity() {
            return city;
        }

        public void setCity(final String city) {
            this.city = city;
        }
    }

    /** A getter-only bean property (accessed by methods, no setter): not a BeanInfo property. */
    @AccessFieldByMethod
    public static class GetterOnlyAddressOwner {
        private Address address = new Address();
        private String name;

        public Address getAddress() {
            return address;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }
    }

    public static class HomeOwner {
        private Address home;

        public Address getHome() {
            return home;
        }

        public void setHome(final Address home) {
            this.home = home;
        }
    }

    /** An intermediate whose class has no no-argument constructor. */
    public record Point(int x) {
    }

    public static class PointOwner {
        private Point point;

        public Point getPoint() {
            return point;
        }

        public void setPoint(final Point point) {
            this.point = point;
        }
    }

    public static class AddressBookOwner {
        private List<Address> addresses;

        public List<Address> getAddresses() {
            return addresses;
        }

        public void setAddresses(final List<Address> addresses) {
            this.addresses = addresses;
        }
    }
}
