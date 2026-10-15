package example.dynamodb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeMap;

// Factory registration deliberately rejects framework-package types, so these user-container fixtures
// live outside com.landawn.abacus to exercise the supported registration API.
public final class ContainerFactoryFixtures {
    private ContainerFactoryFixtures() {
    }

    public static class FactoryMap<K, V> extends TreeMap<K, V> {
        public FactoryMap() {
        }

        public FactoryMap(final Comparator<? super K> comparator) {
            super(comparator);
        }
    }

    public static class FactoryList<E> extends ArrayList<E> {
        private final boolean factoryCreated;

        public FactoryList() {
            factoryCreated = false;
        }

        public FactoryList(final boolean factoryCreated) {
            this.factoryCreated = factoryCreated;
        }

        public boolean isFactoryCreated() {
            return factoryCreated;
        }
    }
}
