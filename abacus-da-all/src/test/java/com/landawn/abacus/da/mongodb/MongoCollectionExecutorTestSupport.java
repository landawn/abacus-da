package com.landawn.abacus.da.mongodb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;

import com.landawn.abacus.da.MockitoTestBase;
import com.landawn.abacus.util.AsyncExecutor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

abstract class MongoCollectionExecutorTestSupport extends MockitoTestBase {

    protected static final String COUNT_COLUMN_CONFLICT = "Group field name 'count' conflicts with the count column of groupByAndCount; use Document as the row type";

    @Mock
    protected MongoCollection<Document> mockCollection;

    @Mock
    protected AsyncExecutor mockAsyncExecutor;

    @Mock
    protected FindIterable<Document> mockFindIterable;

    @Mock
    protected MongoCursor<Document> mockCursor;

    protected MongoCollectionExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new MongoCollectionExecutor(mockCollection, mockAsyncExecutor);

        when(mockCollection.find(any(Bson.class))).thenReturn(mockFindIterable);
        // executeQuery() returns the iterable from coll.find(...) then chains fluent
        // modifiers (projection/sort/skip/limit) — unstubbed they return null and break
        // toEntity(...). Have each chained call return the same mock iterable.
        when(mockFindIterable.projection(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.sort(any())).thenReturn(mockFindIterable);
        when(mockFindIterable.skip(anyInt())).thenReturn(mockFindIterable);
        when(mockFindIterable.limit(anyInt())).thenReturn(mockFindIterable);
        when(mockFindIterable.iterator()).thenReturn(mockCursor);
    }

    protected static class TestEntity {
        protected String id;
        protected String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class GroupRow {
        protected String department;
        protected int count;

        public String getDepartment() {
            return department;
        }

        public void setDepartment(String department) {
            this.department = department;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

    protected void stubFindRows(final List<Document> rows) {
        // iterator()/cursor() for paths that stream the cursor, into(...) for the ones that collect it.
        MongoIterableStubs.stubRows(mockFindIterable, rows);
    }

    public static class DottedRow {
        protected String name;
        protected DottedAddress address;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public DottedAddress getAddress() {
            return address;
        }

        public void setAddress(DottedAddress address) {
            this.address = address;
        }
    }

    public static class DottedAddress {
        protected String city;

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }

    public static class DottedRowWithoutAddress {
        protected String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class CountDocument extends Document {
        protected static final long serialVersionUID = 1L;
    }
}
