#!/usr/bin/env python3
"""Remove dangling `@param <X>` javadoc lines for type parameters a method no longer declares.

A `@param <T>` (or `<ID>`, `<K>`, ...) javadoc tag is dangling when the method/constructor
that the javadoc precedes does not declare `T` as a type parameter. Class/interface-level
javadoc `@param <T>` (documenting the class's own type params) is left untouched.

Dry-run by default: prints file:line of each removal. Pass --apply to edit in place.
"""
import re
import sys
from pathlib import Path

FILES = [
    "abacus-da-all/src/main/java/com/landawn/abacus/da/neo4j/Neo4jExecutor.java",
    "abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/AsyncCassandraExecutorBase.java",
    "abacus-da-all/src/main/java/com/landawn/abacus/da/cassandra/CassandraExecutorBase.java",
    "abacus-da-all/src/main/java/com/landawn/abacus/da/mongodb/AsyncMongoCollectionExecutor.java",
    "abacus-da-all/src/main/java/com/landawn/abacus/da/mongodb/reactivestreams/MongoCollectionExecutor.java",
    "abacus-da-all/src/main/java/com/landawn/abacus/da/mongodb/MongoDBBase.java",
    "abacus-da-all/src/main/java/com/landawn/abacus/da/aws/dynamodb/DynamoDBExecutor.java",
    "abacus-da-all/src/main/java/com/landawn/abacus/da/aws/dynamodb/v2/DynamoDBExecutor.java",
    "abacus-da-all/src/main/java/com/landawn/abacus/da/azure/CosmosContainerExecutor.java",
    "abacus-da-all/src/main/java/com/landawn/abacus/da/hbase/HBaseExecutor.java",
]

# Match a javadoc type-parameter tag: `     * @param <T> ...`
PARAM_TAG = re.compile(r"^(\s*\*\s*@param\s+<)([A-Za-z_][A-Za-z0-9_]*)(>.*$)")

def strip_comments_and_strings(src):
    """Return src with // line comments, block comments, and string/char literals blanked
    (newlines preserved) so scanning for declarations isn't fooled by code inside strings."""
    out = []
    i = 0
    n = len(src)
    in_block = False
    in_line = False
    in_str = False  # "..."
    in_char = False  # '...'
    prev = ''
    while i < n:
        c = src[i]
        if in_block:
            if c == '*' and i+1 < n and src[i+1] == '/':
                in_block = False
                out.append(' ')
                out.append(' ')
                i += 2
                continue
            out.append('\n' if c == '\n' else ' ')
            i += 1
            continue
        if in_line:
            if c == '\n':
                in_line = False
                out.append('\n')
            else:
                out.append('')
            i += 1
            continue
        if in_str:
            if c == '\\' and i+1 < n:
                out.append(' ')
                out.append(' ')
                i += 2
                continue
            if c == '"':
                in_str = False
            out.append('\n' if c == '\n' else ' ')
            i += 1
            continue
        if in_char:
            if c == '\\' and i+1 < n:
                out.append(' ')
                out.append(' ')
                i += 2
                continue
            if c == "'":
                in_char = False
            out.append('\n' if c == '\n' else ' ')
            i += 1
            continue
        # not in any
        if c == '/' and i+1 < n and src[i+1] == '/':
            in_line = True
            i += 2
            continue
        if c == '/' and i+1 < n and src[i+1] == '*':
            in_block = True
            i += 2
            continue
        if c == '"':
            in_str = True
            i += 1
            continue
        if c == "'":
            in_char = True
            i += 1
            continue
        out.append(c)
        i += 1
    return ''.join(out)

def find_method_typeparams(blanke, pos):
    """Given blanked source and a position at/just before a method/constructor declaration
    line, find the first `<...>` clause that precedes the method's `(` return-type/params.
    Returns set of declared type-parameter names, or empty set if the method is non-generic.
    Scans backward from the `(` that opens the parameter list."""
    # Find the '(' that starts the parameter list at or after pos, skipping over nested <>.
    # We look for the first '(' not inside <...> or (...) or [...].
    depth_angle = 0
    i = pos
    n = len(blanke)
    paren_pos = -1
    while i < n:
        c = blanke[i]
        if c == '<':
            depth_angle += 1
        elif c == '>':
            if depth_angle > 0:
                depth_angle -= 1
        elif c == '(' and depth_angle == 0:
            paren_pos = i
            break
        elif c == ';' or c == '{' or c == '}':
            # declaration ended before a paren -> not a method decl (e.g. field)
            return set()
        i += 1
    if paren_pos == -1:
        return set()
    # Now scan backward from paren_pos to find the type-parameter clause `<...>` that
    # belongs to the method. It is the LAST balanced <...> before paren_pos at angle-depth
    # starting 0, that immediately follows the method's modifiers/name region. We scan
    # backward collecting the token just before '('.
    # Strategy: walk backward from paren_pos-1, skip whitespace; if we see '>', it might be
    # the close of a generic-type-args on the return type (e.g. Map<K,V> foo()) — but those
    # are part of the return type, NOT the method type params. The method type params clause
    # is the one that appears BEFORE the return type and name. We need the FIRST <...> clause
    # scanning backward that is a method type-param clause.
    #
    # Heuristic: scan backward, find the method name (identifier just before '('), then the
    # return type, then any type-param clause `<...>` that precedes the return type.
    # Simpler robust approach: find the sequence `... <TYPEPARAMS> RETURNTYPE NAME (`.
    # Scan backward from paren_pos over whitespace then an identifier (name), then whitespace,
    # then the return-type tokens; the method type params clause is a `<...>` that appears
    # before the return type and is NOT followed immediately by an identifier-with-`(`.
    #
    # Given complexity, use a forward scan from the start of the declaration line set:
    # find the FIRST `<...>` clause at depth 0 in the declaration that is followed (after
    # whitespace and tokens) by `(`. The method type params is the first such clause if it
    # appears before the return type. But return type can itself contain `<...>` (e.g.
    # `Collection<T>`). The method type params clause is the one that, after it, there is a
    # return type and name and then `(`.
    #
    # Practical heuristic: the method type-param clause is the first top-level balanced `<...>`
    # in the declaration where the token immediately before it (skipping whitespace) is NOT
    # an identifier/closing-bracket/`>` (i.e. it follows a modifier like `static`/`public`/`final`
    # or a `<` of the previous clause or start). Actually method type params follow modifiers.
    #
    # Simplest reliable method: re-scan forward from `pos`, the FIRST balanced `<...>` at
    # angle-depth 0 whose preceding non-space token is one of the method modifiers or the
    # class context — i.e. it is preceded by a keyword/identifier that is a modifier, or by
    # nothing (start of line). Return types' `<...>` are preceded by an identifier (the type
    # name). So: a `<...>` is the type-param clause iff the token immediately before it
    # (skipping whitespace) is NOT an identifier, NOT `>`, NOT `]`, NOT `)`.
    # Walk forward to find balanced <...> clauses at depth 0.
    i = pos
    clauses = []
    while i < paren_pos:
        c = blanke[i]
        if c == '<':
            # find matching >
            d = 1
            j = i + 1
            while j < paren_pos and d > 0:
                if blanke[j] == '<':
                    d += 1
                elif blanke[j] == '>':
                    d -= 1
                j += 1
            # clause is blanke[i:j]
            # A method type-param clause `<...>` is immediately preceded by whitespace
            # (e.g. `public <T>`, `static <E extends Date>`). A return-type generic like
            # `Collection<T>` has `<` immediately preceded by an identifier char (no space).
            prev_ch = blanke[i - 1] if i > pos else ''
            is_typeparam_clause = (i == pos) or (prev_ch in ' \t\n\r')
            clauses.append((i, j, blanke[i:j], is_typeparam_clause))
            i = j
        else:
            i += 1
    # The method type-param clause is the first clause with is_typeparam_clause True.
    for (s, e, txt, ok) in clauses:
        if ok:
            inner = txt[1:-1]  # strip < >
            names = set()
            for part in split_top_commas(inner):
                part = part.strip()
                m = re.match(r'([A-Za-z_][A-Za-z0-9_]*)', part)
                if m:
                    names.add(m.group(1))
            return names
    return set()

def split_top_commas(s):
    parts = []
    depth = 0
    cur = []
    for c in s:
        if c in '<(':
            depth += 1
            cur.append(c)
        elif c in '>)':
            depth -= 1
            cur.append(c)
        elif c == ',' and depth == 0:
            parts.append(''.join(cur))
            cur = []
        else:
            cur.append(c)
    if cur:
        parts.append(''.join(cur))
    return parts

def process(path, apply):
    src = Path(path).read_text(encoding='utf-8')
    lines = src.split('\n')
    blanke = strip_comments_and_strings(src)
    blines = blanke.split('\n')

    removals = []  # (lineno, text)

    # Find javadoc blocks: a run of consecutive lines starting with `/**` ... `*/`.
    # Then the next non-blank code line is the declaration the javadoc documents.
    i = 0
    N = len(lines)
    while i < N:
        if '*/' in lines[i] and is_javadoc_close(lines, i):
            # find the javadoc block start (nearest preceding `/**`)
            jstart = find_javadoc_start(lines, i)
            if jstart is None:
                i += 1
                continue
            # find next non-blank, non-annotation code line after i (skip `@Foo`/`@Foo(...)`
            # annotation lines that sit between the javadoc and the method declaration)
            k = i + 1
            while k < N:
                s = lines[k].strip()
                if s == '' or re.match(r'@\s*[A-Za-z_][\w.]*(\s*\(|\s*$)', s):
                    k += 1
                    continue
                break
            if k >= N:
                i += 1
                continue
            # Is k a method/constructor declaration? (has a `(` and is not class/interface/enum)
            decl_line_idx, decl_pos_in_blanked = k, None
            # We need the position in `blanke` corresponding to line k start.
            pos = sum(len(blines[m]) + 1 for m in range(k))  # +1 for '\n'
            # Check that the code at k looks like a method/constructor (contains '(' before ';')
            # and not a class/interface/enum/record/@interface declaration.
            code_at_k = blines[k]
            if not looks_like_method_decl(code_at_k, blines, k):
                i += 1
                continue
            tparams = find_method_typeparams(blanke, pos)
            # Scan javadoc block lines jstart..i for `@param <X>` and remove if X not in tparams.
            for ln in range(jstart, i + 1):
                m = PARAM_TAG.match(lines[ln])
                if m:
                    name = m.group(2)
                    if name not in tparams:
                        removals.append((ln + 1, lines[ln].rstrip('\n')))
            i = k  # skip to declaration; don't reprocess its body
            continue
        i += 1

    if not removals:
        print(f"{path}: no dangling @param <X> tags")
        return 0
    if not apply:
        for (ln, txt) in removals:
            print(f"{path}:{ln}: would remove: {txt}")
        return len(removals)
    # apply: remove lines (descending order)
    for (ln, _) in sorted(removals, key=lambda t: -t[0]):
        del lines[ln - 1]
    Path(path).write_text('\n'.join(lines), encoding='utf-8')
    print(f"{path}: removed {len(removals)} dangling @param <X> tag(s)")
    return len(removals)

def is_javadoc_close(lines, i):
    # line contains `*/`; ensure it's part of a javadoc (preceded by `/**` block)
    return True  # caller validates via find_javadoc_start

def find_javadoc_start(lines, close_idx):
    j = close_idx
    while j >= 0:
        if '/**' in lines[j]:
            return j
        if lines[j].lstrip().startswith('//'):
            return None
        if lines[j].strip() != '' and not lines[j].lstrip().startswith('*') and '*/' not in lines[j]:
            # non-javadoc, non-blank line -> not a javadoc block
            return None
        j -= 1
    return None

def looks_like_method_decl(code_line, blines, k):
    s = code_line
    # skip annotations on this and following lines until we hit the actual decl
    # For our purpose: a method/constructor decl line eventually has `<...> ... name(` or
    # `name(` with `(`. Class/interface/enum/record/@interface decls use those keywords and
    # may have `(` for records but not methods. We treat as method if the line (or the joined
    # following lines up to first `{` or `;`) contains `(` and does NOT start (after
    # annotations/modifiers) with class/interface/enum/record/@interface/new.
    # Build joined text from k until we hit `{` or `;` at depth 0.
    joined = []
    depth_angle = 0
    for m in range(k, len(blines)):
        t = blines[m]
        joined.append(t)
        # stop after we see `{` or `;` at angle depth 0 (method body / abstract decl end)
        # simple: if '{' in t or ';' in t: break
        if '{' in t or ';' in t:
            break
    text = ' '.join(joined)
    # strip leading annotations @Foo(...)
    text = re.sub(r'@\s*[A-Za-z_][\w.]*(\s*\([^)]*\))?', '', text, count=0)
    text = text.strip()
    # reject type declarations
    if re.search(r'\b(class|interface|enum|record|@interface)\b', text):
        return False
    # must contain '(' to be a method/constructor
    if '(' not in text:
        return False
    return True

def main():
    apply = '--apply' in sys.argv
    root = Path(__file__).resolve().parent.parent
    total = 0
    for f in FILES:
        total += process(str(root / f), apply)
    print(f"TOTAL {'removed' if apply else 'would remove'}: {total}")

if __name__ == '__main__':
    main()