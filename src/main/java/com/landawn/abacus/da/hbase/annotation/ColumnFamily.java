/*
 * Copyright (c) 2020, Haiyang Li.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.da.hbase.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the column family name for HBase table mapping in entity classes or fields.
 * 
 * <p>This annotation can be applied to both types (classes) and fields to control how 
 * Java objects are mapped to HBase column families and columns. When applied to a class,
 * all fields in that class will be mapped to the specified column family. When applied
 * to a field, only that field will use the specified column family name.</p>
 *
 * <h2>Usage Patterns</h2>
 *
 * <h3>Default Behavior (No Annotation)</h3>
 * <p>Without this annotation, field names are mapped directly to column family names
 * in HBase, with an empty column qualifier:</p>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * public static class Account {
 *     @Id
 *     private String id;  // Maps to HBase: "id:"
 *     private String guid;  // Maps to HBase: "guid:"
 *     private Name name;  // Maps to HBase: "name:firstName", "name:lastName"
 *     private String emailAddress;  // Maps to HBase: "emailAddress:"
 * }
 *
 * public static class Name {
 *     private String firstName;  // Maps to HBase: "name:firstName"
 *     private String lastName;  // Maps to HBase: "name:lastName"
 * }
 * }</pre>
 * 
 * <h3>Class-Level Usage</h3>
 * <p>When applied to a class, all fields become columns within the specified column family:</p>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * @ColumnFamily("user_info")
 * public static class Account {
 *     @Id
 *     private String id;  // Maps to HBase: "user_info:id"
 *     @Column("user_guid")
 *     private String guid;  // Maps to HBase: "user_info:user_guid"
 *     private String emailAddress;  // Maps to HBase: "user_info:emailAddress"
 * }
 * }</pre>
 * 
 * <h3>Field-Level Usage</h3>
 * <p>When applied to individual fields, you can specify different column families
 * for different fields within the same class:</p>
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * @ColumnFamily("basic_info")
 * public static class Account {
 *     @Id
 *     private String id;  // Maps to HBase: "basic_info:id"
 *
 *     @Column("user_guid")
 *     private String guid;  // Maps to HBase: "basic_info:user_guid"
 *
 *     @ColumnFamily("personal")
 *     private Name name;  // Maps to HBase: "personal:firstName", "personal:lastName"
 *
 *     @ColumnFamily("contact")
 *     private String emailAddress;  // Maps to HBase: "contact:emailAddress"
 * }
 *
 * public static class Name {
 *     @Column("given_name")
 *     private String firstName;  // Uses parent's column family: "personal:given_name"
 *     private String lastName;  // Uses parent's column family: "personal:lastName"
 * }
 * }</pre>
 * 
 * <h3>Common Use Cases</h3>
 * <ul>
 *   <li><strong>Data Organization:</strong> Group related fields into logical column families</li>
 *   <li><strong>Performance Optimization:</strong> Place frequently accessed fields together</li>
 *   <li><strong>Schema Evolution:</strong> Separate stable fields from frequently changing ones</li>
 *   <li><strong>Access Control:</strong> Apply different security policies to different column families</li>
 * </ul>
 *
 * @see ElementType#TYPE
 * @see ElementType#FIELD
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.TYPE })
public @interface ColumnFamily {

    /**
     * Specifies the column family name to use in the HBase table.
     * 
     * <p>This value determines the column family portion of the HBase column identifier.
     * The complete column identifier in HBase follows the format "columnFamily:qualifier".</p>
     * 
     * <p><strong>Behavior by target:</strong></p>
     * <ul>
     *   <li><strong>When applied to a class:</strong> All fields in the class will use this
     *       column family name, with field names (or {@code @Column} values) as qualifiers.</li>
     *   <li><strong>When applied to a field:</strong> Only that field will use this column
     *       family name, overriding any class-level column family annotation.</li>
     * </ul>
     * 
     * <p><strong>Default behavior (empty string):</strong></p>
     * <ul>
     *   <li>For primitive fields: field name becomes the column family, qualifier is empty</li>
     *   <li>For entity fields: field name becomes the column family, nested field names become qualifiers</li>
     * </ul>
     * 
     * <p><strong>Naming conventions:</strong></p>
     * <ul>
     *   <li>Use lowercase with underscores: "user_info", "contact_details"</li>
     *   <li>Keep names concise but descriptive</li>
     *   <li>Avoid special characters except underscore</li>
     *   <li>Consider HBase performance implications of column family design</li>
     * </ul>
     * 
     * @return the column family name to use in HBase, or empty string to use default mapping behavior
     */
    String value() default "";
}
