/*
 * Copyright (C) 2026 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Provides synchronous and {@link com.landawn.abacus.util.ContinuableFuture}-based access to Amazon DynamoDB through AWS SDK for Java 1.x.
 * The executors support both native request objects and mapped entity operations; entity {@code Mapper} operations
 * are available on the synchronous executor only.
 *
 * <p>This package also contains {@link com.landawn.abacus.da.aws.dynamodb.DynamoDBExecutorBase}, the SDK-neutral parent
 * of both this package's executor and the AWS SDK for Java 2.x executor in {@code com.landawn.abacus.da.aws.dynamodb.v2};
 * it holds the item-to-bean and value-conversion rules the two share.</p>
 */
package com.landawn.abacus.da.aws.dynamodb;
