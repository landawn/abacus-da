package com.landawn.abacus.da.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.annotation.Id;
import com.landawn.abacus.annotation.Table;
import com.landawn.abacus.query.AbstractQueryBuilder.SP;
import com.landawn.abacus.query.condition.Condition;

public class CassandraExecutorBaseDeepReview2026Test extends CassandraExecutorBaseTestSupport {

    @Test
    @SuppressWarnings("deprecation")
    void compositeIdsPreserveRegisteredKeyOrder() {
        CassandraExecutorBase.registerKeys(OrderedCompositeKeyEntity.class, List.of("partitionKey", "clusteringKey"));

        final Condition condition = TestCassandraExecutor.exposedIdsToCondition(OrderedCompositeKeyEntity.class, "partition-1", "cluster-1");
        final SP statement = executor.exposedPrepareDelete(OrderedCompositeKeyEntity.class, null, condition);

        assertEquals("DELETE FROM deep_review_composite_keys WHERE partition_key = :partitionKey AND clustering_key = :clusteringKey", statement.query());
        assertEquals(List.of("partition-1", "cluster-1"), statement.parameters());
    }

    @Test
    void batchEntityConditionPreservesInputKeyOrder() {
        final BatchKeyEntity first = new BatchKeyEntity();
        first.setId(11L);
        final BatchKeyEntity second = new BatchKeyEntity();
        second.setId(12L);

        final Condition condition = TestCassandraExecutor.exposedEntityToConditionCollection(BatchKeyEntity.class, Arrays.asList(first, second));
        final SP statement = executor.exposedPrepareDelete(BatchKeyEntity.class, null, condition);

        assertEquals("DELETE FROM deep_review_batch_keys WHERE id IN (:id1, :id2)", statement.query());
        assertEquals(List.of(11L, 12L), statement.parameters());
    }

    @Test
    void batchEntityConditionRejectsNullEntity() {
        final BatchKeyEntity entity = new BatchKeyEntity();
        entity.setId(11L);

        assertThrows(IllegalArgumentException.class,
                () -> TestCassandraExecutor.exposedEntityToConditionCollection(BatchKeyEntity.class, Arrays.asList(entity, null)));
    }

    @Table("deep_review_composite_keys")
    public static class OrderedCompositeKeyEntity {
        private String partitionKey;
        private String clusteringKey;

        public String getPartitionKey() {
            return partitionKey;
        }

        public void setPartitionKey(final String partitionKey) {
            this.partitionKey = partitionKey;
        }

        public String getClusteringKey() {
            return clusteringKey;
        }

        public void setClusteringKey(final String clusteringKey) {
            this.clusteringKey = clusteringKey;
        }
    }

    @Table("deep_review_batch_keys")
    public static class BatchKeyEntity {
        @Id
        private Long id;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }
    }
}
