package com.landawn.abacus.da;

import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/** Optional IDE entry point. Maven discovers individual unit test classes directly. */
@Suite
@SelectPackages("com.landawn.abacus.da")
@IncludeClassNamePatterns(".*Test")
public class AbacusDATestSuite {
}
