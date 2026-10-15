package com.landawn.abacus.da.aws.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.DayOfWeek;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.landawn.abacus.util.ImmutableList;
import com.landawn.abacus.util.ImmutableMap;
import com.landawn.abacus.util.Suppliers;

import example.dynamodb.ContainerFactoryFixtures.FactoryList;
import example.dynamodb.ContainerFactoryFixtures.FactoryMap;
import software.amazon.awssdk.core.SdkBytes;

public class DynamoDBContainerFactoryRegressionTest {
    // Registrations are permanent and these fixture classes belong only to this test. Register once per class
    // initialization, so running the test again in the same JVM (for example from both a suite and the class)
    // does not see the second registration's false result.
    private static final boolean MAP_FACTORY_REGISTERED = Suppliers.registerForMap(FactoryMap.class, () -> new FactoryMap<>(String.CASE_INSENSITIVE_ORDER));
    private static final boolean LIST_FACTORY_REGISTERED = Suppliers.registerForCollection(FactoryList.class, () -> new FactoryList<>(true));

    @Test
    public void nativeBinaryAndJsonContainersRetainTheirDifferentFactoryPolicies() {
        assertTrue(MAP_FACTORY_REGISTERED, "The dedicated map fixture must accept its factory before conversion");
        assertTrue(LIST_FACTORY_REGISTERED, "The dedicated collection fixture must accept its factory before conversion");
        final AttributeValue binary = new AttributeValue().withB(ByteBuffer.wrap(new byte[] { 1 }));
        final AttributeValue bean = new AttributeValue().withM(Map.of("name", new AttributeValue().withS("value")));
        final FactoryHolder legacy = DynamoDBExecutor.toEntity(Map.of("beanMap", new AttributeValue().withM(Map.of("A", bean, "a", bean)), "binaryMap",
                new AttributeValue().withM(Map.of("A", binary, "a", binary)), "beanList", new AttributeValue().withL(bean), "binaryList",
                new AttributeValue().withL(binary)), FactoryHolder.class);
        assertFactoryPolicies(legacy);

        final var v2Binary = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteArray(new byte[] { 1 }));
        final var v2Bean = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromM(Map.of("name", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("value")));
        final FactoryHolder v2 = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toEntity(Map.of("beanMap", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("A", v2Bean, "a", v2Bean)), "binaryMap",
                        software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("A", v2Binary, "a", v2Binary)), "beanList",
                        software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(v2Bean)), "binaryList",
                        software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(v2Binary))), FactoryHolder.class);
        assertFactoryPolicies(v2);
    }

    @Test
    public void legacyNestedImmutableAndEnumContainersKeepTheirDeclaredTypes() {
        final AttributeValue bean = new AttributeValue().withM(Map.of("name", new AttributeValue().withS("value")));
        final AttributeValue map = new AttributeValue().withM(Map.of("MONDAY", bean));
        final AttributeValue empty = new AttributeValue().withM(Map.of());
        final ContainerHolder holder = DynamoDBExecutor.toEntity(Map.of("maps", new AttributeValue().withL(map), "lists",
                new AttributeValue().withL(new AttributeValue().withL(bean)), "enums", new AttributeValue().withL(map, empty)), ContainerHolder.class);

        assertNestedContainers(holder);
    }

    @Test
    public void v2NestedImmutableAndEnumContainersKeepTheirDeclaredTypes() {
        final var bean = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromM(Map.of("name", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS("value")));
        final var map = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("MONDAY", bean));
        final var empty = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of());
        final ContainerHolder holder = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toEntity(Map.of("maps", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(map)), "lists",
                        software.amazon.awssdk.services.dynamodb.model.AttributeValue
                                .fromL(List.of(software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(bean)))),
                        "enums", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(map, empty))), ContainerHolder.class);

        assertNestedContainers(holder);
    }

    @Test
    public void immutableAndCopyOnWriteContainersPreserveBinaryValuesOrderAndDuplicates() {
        final ByteBuffer source = ByteBuffer.wrap(new byte[] { 9, 1, 2, -3, 8 }).asReadOnlyBuffer();
        source.position(1);
        source.limit(4);
        final AttributeValue binary = new AttributeValue().withB(source);
        final AttributeValue bean = new AttributeValue().withM(Map.of("buffer", binary));
        final AttributeValue map = new AttributeValue().withM(Map.of("MONDAY", bean));
        final AttributeValue list = new AttributeValue().withL(bean, new AttributeValue().withNULL(true), bean);
        final ContainerHolder legacy = DynamoDBExecutor
                .toEntity(Map.of("maps", new AttributeValue().withL(map), "lists", new AttributeValue().withL(list), "copies", list), ContainerHolder.class);
        assertBinaryContainers(legacy);
        assertSame(source, legacy.getMaps().get(0).get("MONDAY").getBuffer());
        assertEquals(1, source.position());
        assertEquals(4, source.limit());

        final var v2Binary = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB(SdkBytes.fromByteBuffer(source));
        final var v2Bean = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("buffer", v2Binary));
        final var v2Map = software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM(Map.of("MONDAY", v2Bean));
        final var v2List = software.amazon.awssdk.services.dynamodb.model.AttributeValue
                .fromL(List.of(v2Bean, software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromNul(true), v2Bean));
        final ContainerHolder v2 = com.landawn.abacus.da.aws.dynamodb.v2.DynamoDBExecutor
                .toEntity(Map.of("maps", software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(v2Map)), "lists",
                        software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL(List.of(v2List)), "copies", v2List), ContainerHolder.class);
        assertBinaryContainers(v2);
    }

    private static void assertFactoryPolicies(final FactoryHolder holder) {
        assertEquals(2, holder.getBeanMap().size());
        assertEquals(Set.of("A", "a"), holder.getBeanMap().keySet());
        assertEquals("value", holder.getBeanMap().get("A").getName());
        assertEquals("value", holder.getBeanMap().get("a").getName());
        assertNull(holder.getBeanMap().comparator());
        assertFalse(holder.getBeanList().isFactoryCreated());
        assertEquals(1, holder.getBinaryMap().size());
        assertSame(String.CASE_INSENSITIVE_ORDER, holder.getBinaryMap().comparator());
        assertTrue(holder.getBinaryMap().containsKey("A"));
        assertTrue(holder.getBinaryMap().containsKey("a"));
        assertEquals(ByteBuffer.wrap(new byte[] { 1 }), holder.getBinaryMap().get("A"));
        assertTrue(holder.getBinaryList().isFactoryCreated());
        assertEquals(1, holder.getBeanList().size());
        assertEquals("value", holder.getBeanList().get(0).getName());
        assertEquals(ByteBuffer.wrap(new byte[] { 1 }), holder.getBinaryList().get(0));
    }

    private static void assertNestedContainers(final ContainerHolder holder) {
        assertInstanceOf(ImmutableMap.class, holder.getMaps().get(0));
        assertInstanceOf(ImmutableList.class, holder.getLists().get(0));
        assertEquals("value", holder.getMaps().get(0).get("MONDAY").getName());
        assertEquals("value", holder.getLists().get(0).get(0).getName());
        assertInstanceOf(EnumMap.class, holder.getEnums().get(0));
        assertInstanceOf(EnumMap.class, holder.getEnums().get(1));
        assertEquals("value", holder.getEnums().get(0).get(DayOfWeek.MONDAY).getName());
        assertTrue(holder.getEnums().get(1).isEmpty());
    }

    private static void assertBinaryContainers(final ContainerHolder holder) {
        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, -3 }), holder.getMaps().get(0).get("MONDAY").getBuffer());
        assertInstanceOf(CopyOnWriteArrayList.class, holder.getCopies());
        for (final List<Leaf> values : List.of(holder.getLists().get(0), holder.getCopies())) {
            assertEquals(3, values.size());
            assertNull(values.get(1));
            assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, -3 }), values.get(0).getBuffer());
            assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, -3 }), values.get(2).getBuffer());
        }
    }

    public static class FactoryHolder {
        private FactoryMap<String, Leaf> beanMap;
        private FactoryMap<String, ByteBuffer> binaryMap;
        private FactoryList<Leaf> beanList;
        private FactoryList<ByteBuffer> binaryList;

        public FactoryMap<String, Leaf> getBeanMap() {
            return beanMap;
        }

        public void setBeanMap(final FactoryMap<String, Leaf> value) {
            beanMap = value;
        }

        public FactoryMap<String, ByteBuffer> getBinaryMap() {
            return binaryMap;
        }

        public void setBinaryMap(final FactoryMap<String, ByteBuffer> value) {
            binaryMap = value;
        }

        public FactoryList<Leaf> getBeanList() {
            return beanList;
        }

        public void setBeanList(final FactoryList<Leaf> value) {
            beanList = value;
        }

        public FactoryList<ByteBuffer> getBinaryList() {
            return binaryList;
        }

        public void setBinaryList(final FactoryList<ByteBuffer> value) {
            binaryList = value;
        }
    }

    public static class ContainerHolder {
        private List<ImmutableMap<String, Leaf>> maps;
        private List<ImmutableList<Leaf>> lists;
        private List<EnumMap<DayOfWeek, Leaf>> enums;
        private CopyOnWriteArrayList<Leaf> copies;

        public List<ImmutableMap<String, Leaf>> getMaps() {
            return maps;
        }

        public void setMaps(final List<ImmutableMap<String, Leaf>> value) {
            maps = value;
        }

        public List<ImmutableList<Leaf>> getLists() {
            return lists;
        }

        public void setLists(final List<ImmutableList<Leaf>> value) {
            lists = value;
        }

        public List<EnumMap<DayOfWeek, Leaf>> getEnums() {
            return enums;
        }

        public void setEnums(final List<EnumMap<DayOfWeek, Leaf>> value) {
            enums = value;
        }

        public CopyOnWriteArrayList<Leaf> getCopies() {
            return copies;
        }

        public void setCopies(final CopyOnWriteArrayList<Leaf> value) {
            copies = value;
        }
    }

    public static class Leaf {
        private String name;
        private ByteBuffer buffer;

        public String getName() {
            return name;
        }

        public void setName(final String value) {
            name = value;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setBuffer(final ByteBuffer value) {
            buffer = value;
        }
    }
}
