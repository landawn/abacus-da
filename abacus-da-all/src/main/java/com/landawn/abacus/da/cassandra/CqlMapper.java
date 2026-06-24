/*
 * Copyright (C) 2015 HaiYang Li
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

package com.landawn.abacus.da.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import com.landawn.abacus.exception.ParsingException;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.PropertiesUtil;
import com.landawn.abacus.util.XmlUtil;

/**
 * Registry of named CQL (Cassandra Query Language) statements, optionally loaded from XML files.
 *
 * <p>{@code CqlMapper} maps short string identifiers to {@link ParsedCql} values, so application
 * code can refer to queries by name (for example {@code "findAccountById"}) instead of embedding
 * raw CQL strings. The XML loader recognizes a {@code <cqlMapper>} root with one or more
 * {@code <cql id="...">...</cql>} children; the element text is the CQL statement and the
 * remaining XML attributes are passed through to {@link ParsedCql} as a metadata
 * {@code Map<String,String>} (typical entries include {@code timeout}, {@code consistency}, etc.).
 * Note that these attributes are retained for XML round-tripping and as descriptive metadata only:
 * neither executor applies them (timeout, consistency, and so on) to statement execution.</p>
 *
 * <h2>Scope of "mapping"</h2>
 * <p>The mapping performed by this class is <strong>id&nbsp;&rarr;&nbsp;CQL string</strong> only.
 * {@code CqlMapper} does not perform Java&nbsp;&harr;&nbsp;CQL <em>type</em> conversion: that
 * responsibility lies with {@link CassandraExecutor} and the underlying DataStax driver codecs,
 * which translate Java values to and from CQL types ({@code text}, {@code int}, {@code timestamp},
 * {@code uuid}, {@code list}, {@code map}, user-defined types, and so on) at bind / decode time.
 * The CQL text returned from {@link #get(String)} is opaque to this class — it is held as
 * configured and handed to the executor unchanged.</p>
 *
 * <p>Storing CQL externally in XML rather than inline in Java offers:</p>
 *
 * <ul>
 * <li><strong>Separation of Concerns:</strong> Keeps CQL statements separate from Java code</li>
 * <li><strong>Maintainability:</strong> Easy to modify queries without recompiling code</li>
 * <li><strong>Reusability:</strong> CQL statements can be reused across different parts of the application</li>
 * <li><strong>Version Control:</strong> Query changes can be tracked and versioned separately</li>
 * <li><strong>Performance:</strong> Pre-parsed and cached CQL statements for optimal execution</li>
 * </ul>
 *
 * <h2>XML Configuration Format</h2>
 * <h3>Structure</h3>
 * <p>CQL statements are configured in XML files using the following structure:</p>
 * <pre>{@code
 * <cqlMapper>
 *   <cql id="findAccountById" timeout="5000">
 *     SELECT * FROM account WHERE id = ?
 *   </cql>
 *   <cql id="updateAccountNameById">
 *     UPDATE account SET name = ? WHERE id = ?
 *   </cql>
 *   <cql id="insertUser">
 *     INSERT INTO users (id, name, email, created_at)
 *     VALUES (:id, :name, :email, :created_at)
 *   </cql>
 * </cqlMapper>
 * }</pre>
 *
 * <h3>Parameter Binding</h3>
 * <p>The CQL stored in the mapper may use any of the binding styles supported by {@link ParsedCql}:</p>
 * <ul>
 * <li><strong>Positional parameters:</strong> {@code SELECT * FROM users WHERE id = ?}</li>
 * <li><strong>Named parameters:</strong> {@code SELECT * FROM users WHERE id = :userId}</li>
 * <li><strong>MyBatis-style parameters:</strong> {@code SELECT * FROM users WHERE id = #{userId}}</li>
 * </ul>
 * <p>Mixing different parameter styles ({@code ?}, {@code :name}, {@code #{name}}) within a single
 * statement is not supported and is rejected at parse time.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Initialize mapper with XML file
 * CqlMapper mapper = new CqlMapper("classpath:cql-config.xml");
 *
 * // Get parsed CQL statement
 * ParsedCql parsedCql = mapper.get("findAccountById");
 * String cql = parsedCql.originalCql();
 *
 * // Add new CQL statement programmatically
 * Map<String, String> attributes = new HashMap<>();
 * attributes.put("timeout", "3000");
 * mapper.add("findUsersByStatus", "SELECT * FROM users WHERE status = ?", attributes);
 *
 * // Save mapper to XML file
 * mapper.saveTo(new File("updated-cql-config.xml"));
 * }</pre>
 *
 * <h3>Thread Safety</h3>
 * <p>This class is thread-safe for read operations after initialization. Concurrent modifications
 * (add/remove operations) should be externally synchronized.</p>
 *
 * @see ParsedCql
 * @see CassandraExecutor
 * @see CassandraExecutorBase
 */
public final class CqlMapper {

    /**
     * XML element name for the root cqlMapper element.
     */
    public static final String CQL_MAPPER = "cqlMapper";

    /**
     * XML element name for individual CQL statement elements.
     */
    public static final String CQL = "cql";

    /**
     * XML attribute name for the statement identifier.
     */
    public static final String ID = "id";

    static final String TIMEOUT = "timeout";

    private final Map<String, ParsedCql> cqlMap = new LinkedHashMap<>();

    /**
     * Constructs a new empty CqlMapper instance.
     *
     * <p>This default constructor creates an empty CqlMapper with no pre-loaded CQL statements.
     * Statements can be added later either by loading from XML files or by adding them
     * programmatically. This constructor is useful when you want to dynamically load CQL
     * statements based on runtime conditions or when building the mapper incrementally.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper mapper = new CqlMapper();
     * mapper.loadFrom("classpath:user-queries.xml");
     * mapper.loadFrom("classpath:product-queries.xml");
     * }</pre>
     *
     * @see #CqlMapper(String)
     */
    public CqlMapper() {
    }

    /**
     * Constructs a CqlMapper and loads CQL statements from the specified file path.
     * 
     * <p>This constructor immediately loads and parses all CQL statements from the given file,
     * making them available for use. The file should be in the XML format expected by this mapper.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper("classpath:cql-config.xml"); // loads all <cql> entries from the file
     * new CqlMapper((String) null);                            // throws IllegalArgumentException (null path)
     * new CqlMapper("");                                       // throws IllegalArgumentException (empty path)
     * new CqlMapper("/no/such/file.xml");                      // throws RuntimeException (file not found)
     * }</pre>
     *
     * @param filePath the path to the XML file containing CQL statements. Can be:
     *                 <ul>
     *                 <li>Absolute file path: {@code "/path/to/cql-config.xml"}</li>
     *                 <li>Classpath resource: {@code "classpath:cql-config.xml"}</li>
     *                 <li>Multiple paths separated by ',' or ';': {@code "file1.xml,file2.xml"}</li>
     *                 </ul>
     * @throws IllegalArgumentException if {@code filePath} is null or empty, if duplicate CQL IDs
     *         are found, or if a {@code <cql>} element is missing the required {@code id} attribute
     * @throws UncheckedIOException if the file cannot be read or parsed
     * @throws ParsingException if the XML content is malformed
     * @throws RuntimeException if the file is not found, or the required {@code <cqlMapper>} root
     *         element is missing from an input file
     * @see #loadFrom(String)
     */
    public CqlMapper(final String filePath) {
        this();

        loadFrom(filePath);
    }

    /**
     * Loads CQL statements from one or more XML configuration files.
     * 
     * <p>This method supports loading from multiple files by separating file paths with
     * comma ',' or semicolon ';'. All CQL statements from all specified files will be
     * merged into this mapper instance. If duplicate IDs are found across files,
     * an exception will be thrown.</p>
     * 
     * <p>The method automatically detects and handles different file path formats:</p>
     * <ul>
     * <li><strong>Single file:</strong> {@code "cql-config.xml"}</li>
     * <li><strong>Multiple files with comma:</strong> {@code "file1.xml,file2.xml,file3.xml"}</li>
     * <li><strong>Multiple files with semicolon:</strong> {@code "file1.xml;file2.xml;file3.xml"}</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.loadFrom("classpath:user-queries.xml");        // merges entries from one file
     * m.loadFrom("a.xml,b.xml");                       // merges entries from both files
     * m.loadFrom("   ");                               // no-op: blank path resolves to no files, mapper unchanged
     * m.loadFrom((String) null);                       // throws IllegalArgumentException (null path)
     * m.loadFrom("");                                  // throws IllegalArgumentException (empty path)
     * m.loadFrom("/no/such/file.xml");                 // throws RuntimeException (file not found)
     * }</pre>
     *
     * @param filePath single file path or multiple file paths separated by ',' or ';'
     * @throws UncheckedIOException if any file cannot be read
     * @throws ParsingException if any XML file is malformed
     * @throws IllegalArgumentException if {@code filePath} is null or empty, if duplicate CQL IDs
     *         are found, or if a {@code <cql>} element is missing the required {@code id} attribute
     * @throws RuntimeException if a file is not found, or the required {@code <cqlMapper>} root
     *         element is missing from an input file
     */
    public void loadFrom(final String filePath) throws UncheckedIOException {
        N.checkArgNotEmpty(filePath, "filePath");
        final String[] splitPaths = filePath.split("[,;]");
        final List<String> filePaths = new ArrayList<>(splitPaths.length);

        String trimmedPath = null;
        for (final String splitPath : splitPaths) {
            trimmedPath = splitPath.trim();

            if (N.notEmpty(trimmedPath)) {
                filePaths.add(trimmedPath);
            }
        }

        for (final String subFilePath : filePaths) {
            final File file = PropertiesUtil.formatPath(PropertiesUtil.findFile(subFilePath));

            try (InputStream is = new FileInputStream(file)) {

                final Document doc = XmlUtil.createDOMParser(true, true).parse(is);
                final NodeList cqlMapperEle = doc.getElementsByTagName(CqlMapper.CQL_MAPPER);

                if (0 == cqlMapperEle.getLength()) {
                    throw new RuntimeException("Missing required 'cqlMapper' root element in XML configuration");
                }

                final List<Element> cqlElementList = XmlUtil.getElementsByTagName((Element) cqlMapperEle.item(0), CQL);

                for (final Element cqlElement : cqlElementList) {
                    final Map<String, String> attrMap = XmlUtil.readAttributes(cqlElement);
                    final String id = attrMap.remove(ID);

                    if (N.isEmpty(id)) {
                        throw new IllegalArgumentException("Missing required 'id' attribute for CQL element in XML configuration");
                    }

                    add(id, XmlUtil.getTextContent(cqlElement), attrMap);
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            } catch (final SAXException e) {
                throw new ParsingException(e);
            }
        }
    }

    /**
     * Returns the set of all CQL statement identifiers in this mapper.
     *
     * <p>This method provides access to all the IDs that have been loaded into this mapper,
     * allowing you to discover what CQL statements are available. The returned set is a
     * live view of the internal key set, so subsequent {@code add}/{@code remove} calls are
     * reflected in a previously returned set.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.keySet();                                       // returns [] (empty set for an empty mapper)
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * m.add("findAll", "SELECT * FROM users", null);
     * m.keySet();                                       // returns ["findUserById", "findAll"]
     * m.remove("findAll");
     * m.keySet();                                       // returns ["findUserById"] (live view reflects removal)
     * }</pre>
     *
     * @return a set containing all CQL statement IDs currently in this mapper
     */
    public Set<String> keySet() {
        return cqlMap.keySet();
    }

    /**
     * Retrieves a parsed CQL statement by its identifier.
     * 
     * <p>Returns the ParsedCql object associated with the given ID, which contains
     * the original CQL statement, parameterized version, and any associated attributes
     * (such as timeout settings). Returns null if no CQL statement is found for the
     * given ID.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper mapper = new CqlMapper();
     * ParsedCql added = ParsedCql.parse("SELECT * FROM users WHERE id = ?", null);
     * mapper.add("findUserById", added);
     *
     * ParsedCql parsedCql = mapper.get("findUserById");   // returns the stored ParsedCql (same instance)
     * String cql = parsedCql.originalCql();               // "SELECT * FROM users WHERE id = ?"
     *
     * mapper.get("noSuchId");                              // returns null (id not present)
     * mapper.get(null);                                    // returns null (no null key stored)
     * }</pre>
     *
     * @param id the unique identifier of the CQL statement
     * @return the ParsedCql object if found, null otherwise
     * @see ParsedCql
     */
    public ParsedCql get(final String id) {
        return cqlMap.get(id);
    }

    /**
     * Adds a pre-parsed CQL statement to this mapper.
     * 
     * <p>This method allows you to add a ParsedCql object directly to the mapper,
     * which is useful when you have already parsed CQL statements or want to add
     * statements programmatically. If a statement with the same ID already exists,
     * it will be replaced and the previous ParsedCql object will be returned.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper mapper = new CqlMapper();
     * ParsedCql p1 = ParsedCql.parse("SELECT * FROM users WHERE id = ?", null);
     * mapper.add("findUserById", p1);                  // returns null (no previous mapping)
     *
     * ParsedCql p2 = ParsedCql.parse("SELECT name FROM users WHERE id = ?", null);
     * mapper.add("findUserById", p2);                  // returns p1 (id reused; previous value replaced)
     * mapper.get("findUserById");                      // returns p2 (the new mapping)
     * }</pre>
     *
     * @param id the unique identifier for this CQL statement; used as the map key as-is; not validated
     * @param parsedCql the pre-parsed CQL statement object
     * @return the previous ParsedCql associated with the ID, or null if none existed
     * @see ParsedCql
     */
    public ParsedCql add(final String id, final ParsedCql parsedCql) {
        return cqlMap.put(id, parsedCql);
    }

    /**
     * Adds a new CQL statement with attributes to this mapper.
     * 
     * <p>This method parses the provided CQL statement and stores it with the given ID
     * and attributes. The attributes map can contain metadata such as timeout values,
     * consistency levels, or other configuration parameters specific to this CQL statement.</p>
     * 
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper mapper = new CqlMapper();
     * Map<String, String> attrs = new HashMap<>();
     * attrs.put("timeout", "5000");
     * attrs.put("consistency", "QUORUM");
     * mapper.add("insertUser", "INSERT INTO users (id, name) VALUES (?, ?)", attrs);
     * mapper.get("insertUser").parameterCount();       // returns 2
     *
     * mapper.add("plain", "SELECT * FROM users", null); // attrs may be null; stored attributes are empty
     *
     * // re-using an existing id is rejected
     * mapper.add("insertUser", "INSERT INTO users (id) VALUES (?)", null); // throws IllegalArgumentException
     *
     * // null cql is rejected by ParsedCql.parse
     * mapper.add("bad", (String) null, null);          // throws IllegalArgumentException
     * }</pre>
     *
     * @param id the unique identifier for this CQL statement; used as the map key as-is; not validated
     * @param cql the CQL statement string to be parsed and stored
     * @param attrs optional attributes map for statement metadata (can be null)
     * @throws IllegalArgumentException if the ID already exists, if {@code cql} is null,
     *         or if the CQL is invalid
     */
    public void add(final String id, final String cql, final Map<String, String> attrs) {
        if (cqlMap.containsKey(id)) {
            throw new IllegalArgumentException(id + " already exists with cql: " + cqlMap.get(id));
        }

        cqlMap.put(id, ParsedCql.parse(cql, attrs));
    }

    /**
     * Removes a CQL statement from this mapper.
     * 
     * <p>Removes the CQL statement associated with the given ID from this mapper.
     * If no statement exists with the specified ID, this method does nothing.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * m.remove("findUserById");                         // removes the entry; m.get("findUserById") is now null
     * m.isEmpty();                                      // returns true
     *
     * m.remove("noSuchId");                             // no-op (id absent, no exception)
     * m.remove(null);                                   // no-op (no null key present, no exception)
     * }</pre>
     *
     * @param id the identifier of the CQL statement to remove
     */
    public void remove(final String id) {
        cqlMap.remove(id);
    }

    /**
     * Creates a copy of this CqlMapper.
     *
     * <p>Returns a new CqlMapper instance whose internal map contains the same
     * ID-to-{@link ParsedCql} mappings as this mapper. The copy is shallow: the two
     * maps are independent, so adding or removing statements from one will not affect
     * the other, but the {@code ParsedCql} values themselves are shared (not cloned)
     * between the two mappers. This is useful for creating isolated mapper instances
     * for different application contexts.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m1 = new CqlMapper();
     * m1.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * CqlMapper m2 = m1.copy();
     * (m2 == m1);                                       // false (distinct instances)
     * m1.equals(m2);                                    // true (same mappings)
     * m2.remove("findUserById");
     * m1.get("findUserById");                           // still non-null (m1 unaffected by m2's change)
     *
     * new CqlMapper().copy().isEmpty();                 // returns true (copy of an empty mapper is empty)
     * }</pre>
     *
     * @return a new CqlMapper instance containing the same CQL statement mappings
     */
    public CqlMapper copy() {
        final CqlMapper copy = new CqlMapper();

        copy.cqlMap.putAll(cqlMap);

        return copy;
    }

    /**
     * Saves all CQL statements in this mapper to an XML file.
     * 
     * <p>Exports all CQL statements currently stored in this mapper to the specified
     * file in the standard XML format. This is useful for persisting programmatically
     * added CQL statements or creating configuration backups. The generated XML will
     * include all statements with their IDs and attributes.</p>
     * 
     * <p>The output format matches the input format expected by {@link #loadFrom(String)}:</p>
     * <pre>{@code
     * <?xml version="1.0" encoding="UTF-8"?>
     * <cqlMapper>
     *   <cql id="statement1" timeout="3000">
     *     SELECT * FROM table1 WHERE id = ?
     *   </cql>
     *   <cql id="statement2">
     *     INSERT INTO table2 (col1, col2) VALUES (?, ?)
     *   </cql>
     * </cqlMapper>
     * }</pre>
     *
     * <p>If the parent directory of {@code file} does not yet exist, this method attempts to
     * create it.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * m.saveTo(new File("target/cql/config.xml")); // writes the XML; missing parent dir "target/cql" is created
     *
     * // round-trips through loadFrom:
     * CqlMapper reloaded = new CqlMapper("target/cql/config.xml");
     * reloaded.keySet();                            // contains "findUserById"
     *
     * // an unwritable target surfaces as an UncheckedIOException:
     * m.saveTo(new File("/")); // throws UncheckedIOException (cannot write to a directory path)
     * }</pre>
     *
     * @param file the target file where the XML will be written
     * @throws UncheckedIOException if the parent directory cannot be created, or if the file
     *         cannot be written
     * @see #loadFrom(String)
     */
    public void saveTo(final File file) throws UncheckedIOException {
        final File parentFile = file.getParentFile();

        if (parentFile != null && !parentFile.exists() && !parentFile.mkdirs() && !parentFile.exists()) {
            throw new UncheckedIOException(new IOException("Failed to create parent directory: " + parentFile.getAbsolutePath()));
        }

        try (OutputStream os = new FileOutputStream(file)) {
            final Document doc = XmlUtil.createDOMParser(true, true).newDocument();
            final Element cqlMapperNode = doc.createElement(CqlMapper.CQL_MAPPER);

            for (final Map.Entry<String, ParsedCql> cqlEntry : cqlMap.entrySet()) { //NOSONAR
                final String id = cqlEntry.getKey();
                final ParsedCql parsedCql = cqlEntry.getValue();

                final Element cqlNode = doc.createElement(CQL);
                cqlNode.setAttribute(ID, id);

                if (!N.isEmpty(parsedCql.getAttributes())) {
                    final Map<String, String> attrs = parsedCql.getAttributes();

                    for (final Map.Entry<String, String> attrEntry : attrs.entrySet()) {
                        cqlNode.setAttribute(attrEntry.getKey(), attrEntry.getValue());
                    }
                }

                final Text cqlText = doc.createTextNode(parsedCql.originalCql());
                cqlNode.appendChild(cqlText);
                cqlMapperNode.appendChild(cqlNode);
            }

            doc.appendChild(cqlMapperNode);

            XmlUtil.transform(doc, os);

            os.flush();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Checks if this mapper contains any CQL statements.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.isEmpty();                                      // returns true (newly created mapper)
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * m.isEmpty();                                      // returns false (one statement present)
     * m.remove("findUserById");
     * m.isEmpty();                                      // returns true (last statement removed)
     * }</pre>
     *
     * @return {@code true} if this mapper contains no CQL statements, {@code false} otherwise
     */
    public boolean isEmpty() {
        return cqlMap.isEmpty();
    }

    /**
     * Returns the hash code for this CqlMapper.
     * 
     * <p>The hash code is computed based on the internal CQL statements map,
     * ensuring that two CqlMapper instances with the same statements will
     * have the same hash code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * new CqlMapper().hashCode();                       // returns 0 (empty map hashes to 0)
     *
     * CqlMapper m1 = new CqlMapper();
     * CqlMapper m2 = new CqlMapper();
     * m1.add("k", "SELECT 1 FROM t", null);
     * m2.add("k", "SELECT 1 FROM t", null);
     * (m1.hashCode() == m2.hashCode());                 // true (equal mappers share a hash code)
     * }</pre>
     *
     * @return hash code value for this object
     */
    @Override
    public int hashCode() {
        return cqlMap.hashCode();
    }

    /**
     * Indicates whether some other object is "equal to" this CqlMapper.
     * 
     * <p>Two CqlMapper instances are considered equal if they contain the same
     * set of CQL statements with identical IDs and CQL text. Statement attributes do not
     * affect equality, because {@link ParsedCql#equals(Object)} compares only the CQL text.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m1 = new CqlMapper();
     * CqlMapper m2 = new CqlMapper();
     * m1.equals(m1);                                    // returns true (same instance)
     * m1.equals(m2);                                    // returns true (both empty)
     * m1.add("k", "SELECT 1 FROM t", null);
     * m2.add("k", "SELECT 1 FROM t", null);
     * m1.equals(m2);                                    // returns true (same mappings)
     * m1.equals(null);                                  // returns false (null argument)
     * m1.equals("not a mapper");                        // returns false (different type)
     * }</pre>
     *
     * @param obj the reference object with which to compare
     * @return {@code true} if this object is the same as the obj argument; false otherwise
     */
    @Override
    public boolean equals(final Object obj) {
        return this == obj || (obj instanceof CqlMapper && N.equals(((CqlMapper) obj).cqlMap, cqlMap));
    }

    /**
     * Returns a string representation of this CqlMapper.
     * 
     * <p>The string representation includes all CQL statements stored in this mapper,
     * showing their IDs and associated ParsedCql objects. This is primarily useful
     * for debugging and logging purposes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * new CqlMapper().toString();                       // returns "{}" (empty map representation)
     *
     * CqlMapper m = new CqlMapper();
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * m.toString();                                     // contains "findUserById" and the ParsedCql text
     * }</pre>
     *
     * @return a string representation of this object
     */
    @Override
    public String toString() {
        return cqlMap.toString();
    }
}
