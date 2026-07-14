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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import com.landawn.abacus.exception.ParsingException;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.ImmutableMap;
import com.landawn.abacus.util.ImmutableSet;
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
 * remaining XML attributes are stored in this mapper as per-id metadata (a
 * {@code Map<String,String>} retrievable via {@link #getAttributes(String)}; typical entries
 * include {@code timeout}, {@code consistency}, etc.).
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
 * CqlMapper mapper = CqlMapper.loadFrom("config/cql-config.xml");
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

    static final Logger logger = LoggerFactory.getLogger(CqlMapper.class);

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

    private final Map<String, ParsedCql> cqlMap = new LinkedHashMap<>();

    private final Map<String, ImmutableMap<String, String>> attrsMap = new HashMap<>();

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
     * mapper.add("findUserById", "SELECT * FROM users WHERE id = ?");
     * mapper.add("findProductById", "SELECT * FROM products WHERE id = ?");
     * }</pre>
     *
     * @see #loadFrom(String)
     */
    public CqlMapper() {
    }

    /**
     * Creates a new {@code CqlMapper} by loading CQL definitions from one or more XML files.
     *
     * <p>Multiple file paths may be supplied separated by comma ',' or semicolon ';'. Definitions
     * from all files are merged into the returned mapper; duplicate ids (within or across files) are
     * rejected.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = CqlMapper.loadFrom("config/cql-config.xml");     // single file
     * CqlMapper both = CqlMapper.loadFrom("a.xml,b.xml");            // merges entries from both files
     * CqlMapper.loadFrom((String) null);                            // throws IllegalArgumentException (null path)
     * CqlMapper.loadFrom("");                                       // throws IllegalArgumentException (empty path)
     * CqlMapper.loadFrom("   ");                                    // throws IllegalArgumentException (no paths after splitting)
     * CqlMapper.loadFrom("/no/such/file.xml");                      // throws RuntimeException (file not found)
     * }</pre>
     *
     * @param filePath single file path or multiple file paths separated by ',' or ';'
     * @return a new CqlMapper populated with the loaded CQL definitions
     * @throws IllegalArgumentException if {@code filePath} is null/empty, resolves to no paths after splitting,
     *         if duplicate CQL ids are found, if a {@code <cql>} element is missing its {@code id} attribute,
     *         or if a CQL statement in a file cannot be parsed (see {@link ParsedCql#parse(String)})
     * @throws UncheckedIOException if a file cannot be read
     * @throws ParsingException if any XML file is malformed
     * @throws RuntimeException if a file is not found, or the required {@code <cqlMapper>} root element is missing
     * @see #loadFrom(File...)
     */
    public static CqlMapper loadFrom(final String filePath) {
        N.checkArgNotEmpty(filePath, "filePath");

        final List<String> filePaths = splitFilePaths(filePath);

        if (filePaths.isEmpty()) {
            throw new IllegalArgumentException("File path is empty after splitting: " + filePath);
        }

        final CqlMapper cqlMapper = new CqlMapper();

        for (final String subFilePath : filePaths) {
            final File file = PropertiesUtil.findFile(subFilePath);

            // findFile returns null when nothing matches; fail with a descriptive message instead
            // of the bare NPE that formatPath(null) would raise.
            if (file == null) {
                throw new RuntimeException("File not found: " + subFilePath);
            }

            cqlMapper.loadFile(PropertiesUtil.formatPath(file));
        }

        return cqlMapper;
    }

    /**
     * Creates a new {@code CqlMapper} by loading CQL definitions from one or more XML files.
     * Each file must contain a {@code <cqlMapper>} root element; definitions from all files are merged
     * into the returned mapper. Duplicate ids across files are rejected.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = CqlMapper.loadFrom(new File("a.xml"), new File("b.xml"));
     * }</pre>
     *
     * @param files one or more XML files to load (must be non-empty and contain no null element)
     * @return a new CqlMapper populated with the loaded CQL definitions
     * @throws IllegalArgumentException if {@code files} is null or empty, if any element is null,
     *         if duplicate CQL ids are found, if a {@code <cql>} element is missing its {@code id} attribute,
     *         or if a CQL statement in a file cannot be parsed (see {@link ParsedCql#parse(String)})
     * @throws UncheckedIOException if a file cannot be read
     * @throws ParsingException if any XML file is malformed
     * @throws RuntimeException if the required {@code <cqlMapper>} root element is missing
     */
    public static CqlMapper loadFrom(final File... files) {
        N.checkArgNotEmpty(files, "files");

        final CqlMapper cqlMapper = new CqlMapper();

        for (final File file : files) {
            N.checkArgNotNull(file, "file");
            cqlMapper.loadFile(file);
        }

        return cqlMapper;
    }

    /**
     * Creates a new {@code CqlMapper} by loading CQL definitions from the supplied input stream.
     * The stream content must contain a {@code <cqlMapper>} root element. The caller opens the stream
     * and remains responsible for closing it (typically via try-with-resources); this method does not
     * close it.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try (InputStream is = new FileInputStream("cql-config.xml")) {
     *     CqlMapper m = CqlMapper.loadFrom(is);
     * }
     * }</pre>
     *
     * @param is the input stream to read the XML CQL definitions from (must not be null)
     * @return a new CqlMapper populated with the loaded CQL definitions
     * @throws IllegalArgumentException if {@code is} is null, if duplicate CQL ids are found,
     *         if a {@code <cql>} element is missing its {@code id} attribute, or if a CQL
     *         statement cannot be parsed (see {@link ParsedCql#parse(String)})
     * @throws UncheckedIOException if an I/O error occurs reading the stream
     * @throws ParsingException if the XML content is malformed
     * @throws RuntimeException if the required {@code <cqlMapper>} root element is missing
     */
    public static CqlMapper loadFrom(final InputStream is) {
        N.checkArgNotNull(is, "is");

        final CqlMapper cqlMapper = new CqlMapper();
        cqlMapper.loadStream(is, "input stream");

        return cqlMapper;
    }

    /**
     * Splits a comma/semicolon-separated list of file paths into trimmed, non-empty entries.
     *
     * @param filePath the raw path expression (one or more paths separated by ',' or ';')
     * @return the list of trimmed, non-empty file paths (possibly empty if all entries are blank)
     */
    private static List<String> splitFilePaths(final String filePath) {
        final String[] splitPaths = filePath.split("[,;]");
        final List<String> filePaths = new ArrayList<>(splitPaths.length);

        for (final String splitPath : splitPaths) {
            final String trimmedPath = splitPath.trim();

            if (N.notEmpty(trimmedPath)) {
                filePaths.add(trimmedPath);
            }
        }

        return filePaths;
    }

    /**
     * Loads the CQL definitions from {@code file} into this mapper, opening and closing the file stream.
     *
     * @param file the XML file to read
     * @throws UncheckedIOException if an I/O error occurs reading the file
     * @throws ParsingException if the XML content is malformed
     */
    private void loadFile(final File file) {
        if (logger.isInfoEnabled()) {
            logger.info("Loading CQL mapper from file: {}", file.getAbsolutePath());
        }

        try (InputStream is = new FileInputStream(file)) {
            loadStream(is, file.getAbsolutePath());
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Parses the XML read from {@code is} and merges its {@code <cql>} definitions into this mapper.
     * The stream is not closed by this method; the caller owns and closes it.
     *
     * @param is the input stream to read the XML CQL definitions from
     * @param sourceLabel a human-readable label identifying the source, used in error messages
     * @throws UncheckedIOException if an I/O error occurs while reading the stream
     * @throws ParsingException if the XML content is malformed
     * @throws RuntimeException if the required {@code <cqlMapper>} root element is missing
     * @throws IllegalArgumentException if a {@code <cql>} element is missing its {@code id} attribute,
     *         or if a duplicate id is encountered
     */
    private void loadStream(final InputStream is, final String sourceLabel) {
        try {
            final Document doc = XmlUtil.createDOMParser(true, true).parse(is);
            final NodeList cqlMapperEle = doc.getElementsByTagName(CqlMapper.CQL_MAPPER);

            if (0 == cqlMapperEle.getLength()) {
                throw new RuntimeException("Missing required 'cqlMapper' root element in: " + sourceLabel);
            }

            final List<Element> cqlElementList = XmlUtil.getElementsByTagName((Element) cqlMapperEle.item(0), CQL);

            for (final Element cqlElement : cqlElementList) {
                final Map<String, String> attrMap = XmlUtil.readAttributes(cqlElement);
                final String id = attrMap.remove(ID);

                if (N.isEmpty(id)) {
                    throw new IllegalArgumentException("Missing required 'id' attribute for CQL element in: " + sourceLabel);
                }

                add(id, XmlUtil.getTextContent(cqlElement), attrMap);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Loaded {} CQL statements from: {}", cqlElementList.size(), sourceLabel);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        } catch (final SAXException e) {
            throw new ParsingException(e);
        }
    }

    /**
     * Returns the set of all CQL statement identifiers in this mapper.
     *
     * <p>This method provides access to all the IDs that have been loaded into this mapper,
     * allowing you to discover what CQL statements are available. The returned set is a
     * read-only snapshot taken at call time; subsequent {@code add}/{@code remove} calls are
     * not reflected in a previously returned set. Attempts to modify the returned set (or its iterator)
     * throw {@link UnsupportedOperationException}; use {@link #add} / {@link #remove} to change the
     * mapper's contents.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.ids();                                       // returns [] (empty set for an empty mapper)
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * m.add("findAll", "SELECT * FROM users", null);
     * m.ids();                                       // returns a set containing "findUserById" and "findAll"
     * m.remove("findAll");
     * m.ids();                                       // returns ["findUserById"] (a fresh snapshot after removal)
     * m.ids().clear();                              // throws UnsupportedOperationException (read-only set)
     * }</pre>
     *
     * @return a read-only snapshot of the set of all CQL statement IDs currently in this mapper
     */
    public ImmutableSet<String> ids() {
        return ImmutableSet.copyOf(cqlMap.keySet());
    }

    /**
     * Retrieves a parsed CQL statement by its identifier.
     *
     * <p>Returns the ParsedCql object associated with the given ID, which contains
     * the original CQL statement and its parameterized version. Any metadata attributes
     * are stored separately on the mapper and are retrievable via {@link #getAttributes(String)}.
     * Returns null if no CQL statement is found for the given ID.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper mapper = new CqlMapper();
     * ParsedCql added = ParsedCql.parse("SELECT * FROM users WHERE id = ?");
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
     * Returns {@code true} if this mapper contains a CQL statement registered under the specified identifier.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * m.containsId("findUserById");                     // returns true
     * m.containsId("noSuchId");                         // returns false (id absent)
     * m.containsId(null);                               // returns false (no null key present)
     * }</pre>
     *
     * @param id the identifier of the CQL statement to test
     * @return {@code true} if a CQL statement is registered under {@code id}, {@code false} otherwise
     */
    public boolean containsId(final String id) {
        return cqlMap.containsKey(id);
    }

    /**
     * Adds a pre-parsed CQL statement to this mapper, with no metadata attributes.
     *
     * <p>This method allows you to add a ParsedCql object directly to the mapper,
     * which is useful when you have already parsed CQL statements or want to add
     * statements programmatically. If a statement with the same ID already exists,
     * an {@link IllegalArgumentException} is thrown — use {@link #remove(String)} first to
     * replace an existing mapping. This is a convenience overload of
     * {@link #add(String, ParsedCql, Map)} with an empty attribute map.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper mapper = new CqlMapper();
     * ParsedCql p1 = ParsedCql.parse("SELECT * FROM users WHERE id = ?");
     * mapper.add("findUserById", p1);                  // stored (no previous mapping)
     *
     * ParsedCql p2 = ParsedCql.parse("SELECT name FROM users WHERE id = ?");
     * mapper.add("findUserById", p2);                  // throws IllegalArgumentException (id already exists)
     *
     * mapper.add("other", (ParsedCql) null);           // throws IllegalArgumentException (parsedCql is null)
     * }</pre>
     *
     * @param id the non-empty unique identifier for this CQL statement
     * @param parsedCql the pre-parsed CQL statement object (must not be null)
     * @throws IllegalArgumentException if {@code id} is null or empty, the id already exists,
     *         or if {@code parsedCql} is null
     * @see ParsedCql
     * @see #add(String, ParsedCql, Map)
     */
    public void add(final String id, final ParsedCql parsedCql) {
        add(id, parsedCql, null);
    }

    /**
     * Adds a pre-parsed CQL statement together with its metadata attributes to this mapper.
     *
     * <p>If a statement with the same ID already exists, an {@link IllegalArgumentException} is
     * thrown — use {@link #remove(String)} first to replace an existing mapping. The attributes are
     * stored on the mapper, keyed by id, and are retrievable via {@link #getAttributes(String)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper mapper = new CqlMapper();
     * Map<String, String> attrs = new HashMap<>();
     * attrs.put("timeout", "5000");
     * mapper.add("findUserById", ParsedCql.parse("SELECT * FROM users WHERE id = ?"), attrs);
     * mapper.getAttributes("findUserById").get("timeout");   // returns "5000"
     *
     * // re-using an existing id is rejected
     * mapper.add("findUserById", ParsedCql.parse("SELECT 1 FROM users"), null); // throws IllegalArgumentException
     *
     * // a null statement is rejected
     * mapper.add("bad", (ParsedCql) null, null);             // throws IllegalArgumentException
     * }</pre>
     *
     * @param id the non-empty unique identifier for this CQL statement
     * @param parsedCql the pre-parsed CQL statement object (must not be null)
     * @param attrs optional attributes map for statement metadata (may be null or empty)
     * @throws IllegalArgumentException if {@code id} is null or empty, the id already exists,
     *         or {@code parsedCql} is null
     * @see ParsedCql
     */
    public void add(final String id, final ParsedCql parsedCql, final Map<String, String> attrs) {
        N.checkArgNotNull(parsedCql, "parsedCql");
        checkId(id);
        checkDuplicateId(id);

        cqlMap.put(id, parsedCql);
        attrsMap.put(id, N.isEmpty(attrs) ? ImmutableMap.empty() : ImmutableMap.copyOf(attrs));
    }

    /**
     * Adds a CQL statement string to this mapper, with no metadata attributes.
     *
     * <p>The CQL string is parsed via {@link ParsedCql#parse(String)} before being stored. This is a
     * convenience overload of {@link #add(String, String, Map)} with an empty attribute map.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper mapper = new CqlMapper();
     * mapper.add("findAll", "SELECT * FROM users");
     * }</pre>
     *
     * @param id the non-empty unique identifier for this CQL statement
     * @param cql the CQL statement string to be parsed and stored
     * @throws IllegalArgumentException if {@code id} is null or empty, the ID already exists, if {@code cql} is null,
     *         or if the CQL is invalid
     */
    public void add(final String id, final String cql) {
        add(id, cql, null);
    }

    /**
     * Adds a new CQL statement with attributes to this mapper.
     *
     * <p>This method parses the provided CQL statement and stores it with the given ID,
     * along with the supplied attributes. The attributes map can contain metadata such as timeout
     * values, consistency levels, or other configuration parameters specific to this CQL statement;
     * they are stored on the mapper, keyed by id, and are retrievable via {@link #getAttributes(String)}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper mapper = new CqlMapper();
     * Map<String, String> attrs = new HashMap<>();
     * attrs.put("timeout", "5000");
     * attrs.put("consistency", "QUORUM");
     * mapper.add("insertUser", "INSERT INTO users (id, name) VALUES (?, ?)", attrs);
     * mapper.get("insertUser").parameterCount();       // returns 2
     * mapper.getAttributes("insertUser").get("timeout"); // returns "5000"
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
     * @param id the non-empty unique identifier for this CQL statement
     * @param cql the CQL statement string to be parsed and stored
     * @param attrs optional attributes map for statement metadata (can be null)
     * @throws IllegalArgumentException if {@code id} is null or empty, the ID already exists,
     *         {@code cql} is null, or the CQL is invalid
     */
    public void add(final String id, final String cql, final Map<String, String> attrs) {
        checkId(id);
        checkDuplicateId(id);

        cqlMap.put(id, ParsedCql.parse(cql));
        attrsMap.put(id, N.isEmpty(attrs) ? ImmutableMap.empty() : ImmutableMap.copyOf(attrs));
    }

    /**
     * Throws {@link IllegalArgumentException} if a CQL statement is already registered under {@code id}.
     *
     * <p>All {@code add} overloads reject duplicate ids; call {@link #remove(String)} first to replace
     * an existing mapping.</p>
     *
     * @param id the identifier to test
     * @throws IllegalArgumentException if {@code id} is already present in this mapper
     */
    private void checkDuplicateId(final String id) {
        if (cqlMap.containsKey(id)) {
            throw new IllegalArgumentException(id + " already exists with cql: " + cqlMap.get(id));
        }
    }

    /** Validates identifiers consistently for programmatic and XML-loaded mappings. */
    private static void checkId(final String id) {
        N.checkArgNotEmpty(id, "id");
    }

    /**
     * Retrieves the metadata attributes registered for the specified CQL statement id.
     *
     * <p>Attributes are descriptive metadata carried alongside the CQL statement (typically loaded from
     * the XML configuration); common entries include {@code timeout} and {@code consistency}. They are
     * retained for XML round-tripping and as metadata only — neither executor applies them to statement
     * execution.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * Map<String, String> attrs = new HashMap<>();
     * attrs.put("timeout", "3000");
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", attrs);
     * m.getAttributes("findUserById").get("timeout");   // returns "3000"
     *
     * m.add("findAll", "SELECT * FROM users");
     * m.getAttributes("findAll").isEmpty();             // returns true (present id, no attributes)
     *
     * m.getAttributes("noSuchId");                      // returns null (id not present)
     * }</pre>
     *
     * @param id the identifier of the CQL statement
     * @return an immutable map of attribute name-value pairs (empty if the id has no attributes),
     *         or {@code null} if the id is not present in this mapper
     */
    public ImmutableMap<String, String> getAttributes(final String id) {
        return attrsMap.get(id);
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
        attrsMap.remove(id);
    }

    /**
     * Creates a copy of this CqlMapper.
     *
     * <p>Returns a new CqlMapper instance whose internal maps contain the same
     * ID-to-{@link ParsedCql} mappings and ID-to-attributes mappings as this mapper. The copy is
     * shallow: the two mappers are independent, so adding or removing statements from one will not
     * affect the other, but the {@code ParsedCql} values (and the immutable attribute maps)
     * themselves are shared (not cloned) between the two mappers. This is useful for creating
     * isolated mapper instances for different application contexts.</p>
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
        copy.attrsMap.putAll(attrsMap);

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
     * <p>The registered identifier (the map key) is always written as the {@code id} attribute and is
     * protected from being overridden: any stray {@code id} entry in a statement's attributes map is
     * ignored when emitting attributes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * m.saveTo(new File("target/cql/config.xml")); // writes the XML; missing parent dir "target/cql" is created
     *
     * // round-trips through load:
     * CqlMapper reloaded = CqlMapper.loadFrom("target/cql/config.xml");
     * reloaded.ids();                            // contains "findUserById"
     *
     * // an unwritable target surfaces as an UncheckedIOException:
     * m.saveTo(new File("/")); // throws UncheckedIOException (cannot write to a directory path)
     * }</pre>
     *
     * @param file the target file where the XML will be written
     * @throws IllegalArgumentException if {@code file} is null
     * @throws UncheckedIOException if the parent directory cannot be created, or if the file
     *         cannot be written
     * @see #saveTo(OutputStream)
     * @see #loadFrom(String)
     */
    public void saveTo(final File file) throws UncheckedIOException {
        N.checkArgNotNull(file, "file");

        final File parentFile = file.getParentFile();

        if (parentFile != null && !parentFile.exists() && !parentFile.mkdirs() && !parentFile.exists()) {
            throw new UncheckedIOException(new IOException("Failed to create parent directory: " + parentFile.getAbsolutePath()));
        }

        try (OutputStream os = new FileOutputStream(file)) {
            saveTo(os);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes all CQL statements in this mapper to the supplied output stream as XML.
     *
     * <p>The output format matches the input format expected by {@link #loadFrom(String)}. The stream is
     * flushed but <i>not</i> closed by this method; the caller retains ownership and is responsible for
     * closing it.</p>
     *
     * <p>The registered identifier (the map key) is always written as the {@code id} attribute and is
     * protected from being overridden: any stray {@code id} entry in a statement's attributes map is
     * ignored when emitting attributes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * try (OutputStream os = new FileOutputStream("target/cql/config.xml")) {
     *     m.saveTo(os);
     * }
     * }</pre>
     *
     * @param os the output stream to write to (not closed by this method)
     * @throws IllegalArgumentException if {@code os} is null
     * @throws UncheckedIOException if an I/O error occurs while writing to the stream
     * @see #saveTo(File)
     * @see #loadFrom(String)
     */
    public void saveTo(final OutputStream os) throws UncheckedIOException {
        N.checkArgNotNull(os, "os");

        try {
            final Document doc = XmlUtil.createDOMParser(true, true).newDocument();
            final Element cqlMapperNode = doc.createElement(CqlMapper.CQL_MAPPER);

            for (final Map.Entry<String, ParsedCql> cqlEntry : cqlMap.entrySet()) { //NOSONAR
                final String id = cqlEntry.getKey();
                final ParsedCql parsedCql = cqlEntry.getValue();

                final Element cqlNode = doc.createElement(CQL);

                final Map<String, String> attrs = attrsMap.get(id);

                if (!N.isEmpty(attrs)) {
                    for (final Map.Entry<String, String> attrEntry : attrs.entrySet()) {
                        // Skip any stray "id" attribute so it cannot overwrite the canonical id set below.
                        if (ID.equals(attrEntry.getKey())) {
                            continue;
                        }

                        cqlNode.setAttribute(attrEntry.getKey(), attrEntry.getValue());
                    }
                }

                // Set the id last to guarantee the entry key wins regardless of the attribute contents.
                cqlNode.setAttribute(ID, id);

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
     * Returns the number of CQL statements registered in this mapper.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m = new CqlMapper();
     * m.size();                                         // returns 0 (newly created mapper)
     * m.add("findUserById", "SELECT * FROM users WHERE id = ?", null);
     * m.size();                                         // returns 1
     * }</pre>
     *
     * @return the number of registered CQL statements
     */
    public int size() {
        return cqlMap.size();
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
     * <p>The hash code is computed from the internal CQL statements map and the per-id attributes
     * map, ensuring that two CqlMapper instances with the same statements and attributes will
     * have the same hash code.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m1 = new CqlMapper();
     * CqlMapper m2 = new CqlMapper();
     * (m1.hashCode() == m2.hashCode());                 // true (two empty mappers share a hash code)
     * m1.add("k", "SELECT 1 FROM t", null);
     * m2.add("k", "SELECT 1 FROM t", null);
     * (m1.hashCode() == m2.hashCode());                 // true (equal mappers share a hash code)
     * }</pre>
     *
     * @return hash code value for this object
     */
    @Override
    public int hashCode() {
        return Objects.hash(cqlMap, attrsMap);
    }

    /**
     * Indicates whether some other object is "equal to" this CqlMapper.
     *
     * <p>Two CqlMapper instances are considered equal if they contain the same set of CQL
     * statements (identical IDs and CQL text) and the same per-id metadata attributes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CqlMapper m1 = new CqlMapper();
     * CqlMapper m2 = new CqlMapper();
     * m1.equals(m1);                                    // returns true (same instance)
     * m1.equals(m2);                                    // returns true (both empty)
     * m1.add("k", "SELECT 1 FROM t", null);
     * m2.add("k", "SELECT 1 FROM t", null);
     * m1.equals(m2);                                    // returns true (same mappings and attributes)
     * m1.equals(null);                                  // returns false (null argument)
     * m1.equals("not a mapper");                        // returns false (different type)
     * }</pre>
     *
     * @param obj the reference object with which to compare
     * @return {@code true} if this object is the same as the obj argument; false otherwise
     */
    @Override
    public boolean equals(final Object obj) {
        return this == obj || (obj instanceof CqlMapper && N.equals(((CqlMapper) obj).cqlMap, cqlMap) && N.equals(((CqlMapper) obj).attrsMap, attrsMap));
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
