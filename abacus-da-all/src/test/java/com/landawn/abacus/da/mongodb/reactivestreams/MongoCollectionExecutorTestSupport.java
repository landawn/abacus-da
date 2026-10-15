package com.landawn.abacus.da.mongodb.reactivestreams;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.landawn.abacus.da.TestBase;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import com.mongodb.reactivestreams.client.MongoCollection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
abstract class MongoCollectionExecutorTestSupport extends TestBase {

    protected static final String COUNT_COLUMN_CONFLICT = "Group field name 'count' conflicts with the count column of groupByAndCount; use Document as the row type";

    @Mock
    protected MongoCollection<Document> mockCollection;

    @Mock
    protected FindPublisher<Document> mockFindPublisher;

    @Mock
    protected AggregatePublisher<Document> mockAggregatePublisher;

    @Mock
    protected DistinctPublisher<String> mockDistinctPublisher;

    @Mock
    protected MapReducePublisher<Document> mockMapReducePublisher;

    @Mock
    protected ChangeStreamPublisher<Document> mockChangeStreamPublisher;

    protected MongoCollectionExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new MongoCollectionExecutor(mockCollection);
    }

    @SuppressWarnings("unchecked")
    @SafeVarargs
    protected static <T> void stubEmits(final Publisher<T> publisher, final T... items) {
        // The executor consumes these publishers via Flux.from(publisher), which calls
        // publisher.subscribe(...). Stubbing publisher.first()/limit()/etc. is useless —
        // those are never invoked. We must stub subscribe() itself so the subscriber
        // actually receives signals, otherwise StepVerifier hangs forever.
        doAnswer(invocation -> {
            Flux.fromArray(items).subscribe((Subscriber<? super T>) invocation.getArgument(0));
            return null;
        }).when(publisher).subscribe(any());
    }

    public static class CountingBean {
        protected int readCount;

        public String getName() {
            readCount++;
            return "counted";
        }

        public void setName(final String name) {
            // setter required for the bean conversion path
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

    protected void stubFindChain(final Bson filter) {
        when(mockCollection.find(filter)).thenReturn(mockFindPublisher);
        when(mockFindPublisher.projection(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.sort(any(Bson.class))).thenReturn(mockFindPublisher);
        when(mockFindPublisher.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(mockFindPublisher);
        when(mockFindPublisher.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(mockFindPublisher);
    }

    public static class CountDocument extends Document {
        protected static final long serialVersionUID = 1L;
    }

    // A collection whose every read (find, aggregate, findOneAndUpdate/Delete) emits the given row on each subscription.
    @SuppressWarnings("unchecked")
    public static MongoCollection<Document> mockCollectionReturning(final Document row) {
        final org.mockito.MockSettings lenient = org.mockito.Mockito.withSettings().strictness(org.mockito.quality.Strictness.LENIENT);
        final MongoCollection<Document> collection = mock(MongoCollection.class, lenient);
        final FindPublisher<Document> findPublisher = mock(FindPublisher.class, lenient);
        final AggregatePublisher<Document> aggregatePublisher = mock(AggregatePublisher.class, lenient);
        when(collection.find(any(Bson.class))).thenReturn(findPublisher);
        when(findPublisher.projection(any())).thenReturn(findPublisher);
        when(findPublisher.sort(any())).thenReturn(findPublisher);
        when(findPublisher.skip(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        when(findPublisher.limit(org.mockito.ArgumentMatchers.anyInt())).thenReturn(findPublisher);
        when(collection.aggregate(anyList(), eq(Document.class))).thenReturn(aggregatePublisher);
        when(collection.findOneAndUpdate(any(Bson.class), any(Bson.class))).thenReturn(Mono.just(row));
        when(collection.findOneAndDelete(any(Bson.class))).thenReturn(Mono.just(row));

        for (final Publisher<Document> publisher : Arrays.<Publisher<Document>> asList(findPublisher, aggregatePublisher)) {
            doAnswer(invocation -> {
                Flux.just(row).subscribe((Subscriber<? super Document>) invocation.getArgument(0));
                return null;
            }).when(publisher).subscribe(any());
        }

        return collection;
    }
}
