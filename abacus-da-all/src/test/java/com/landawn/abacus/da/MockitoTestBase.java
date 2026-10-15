package com.landawn.abacus.da;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.MockitoAnnotations;

/** Owns annotation-created mocks without changing the strictness of existing tests. */
public abstract class MockitoTestBase extends TestBase {
    private AutoCloseable mocks;

    @BeforeEach
    final void openTestMocks() {
        mocks = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    final void closeTestMocks() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }
}
