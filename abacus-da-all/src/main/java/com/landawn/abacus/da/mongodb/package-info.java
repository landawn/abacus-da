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
 * Provides blocking MongoDB database, collection, and entity-mapping operations using the synchronous Java driver.
 * It also includes {@link com.landawn.abacus.util.ContinuableFuture}-based wrappers that execute blocking operations through an asynchronous
 * executor.
 */
package com.landawn.abacus.da.mongodb;
