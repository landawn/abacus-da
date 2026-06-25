import os, re, sys

ROOT = r"abacus-da-all/src/main/java/com/landawn/abacus/da"
MODIFIERS = {"public","protected","private","static","final","synchronized",
             "abstract","default","native","strictfp","transient","volatile"}

def clean(src):
    out = []
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c == '/' and i+1 < n and src[i+1] == '/':
            while i < n and src[i] != '\n':
                i += 1
        elif c == '/' and i+1 < n and src[i+1] == '*':
            i += 2
            while i+1 < n and not (src[i]=='*' and src[i+1]=='/'):
                i += 1
            i += 2
        elif c == '"':
            i += 1
            while i < n and src[i] != '"':
                i += 2 if src[i]=='\\' else 1
            i += 1
        elif c == "'":
            i += 1
            while i < n and src[i] != "'":
                i += 2 if src[i]=='\\' else 1
            i += 1
        else:
            out.append(c); i += 1
    return ''.join(out)

def match_angle(src, i):
    depth = 0
    j = i
    while j < len(src):
        if src[j] == '<': depth += 1
        elif src[j] == '>':
            depth -= 1
            if depth == 0: return j
        elif src[j] == '\n':  # type-param blocks don't span lines in declarations; bail to avoid runaway
            return -1
        j += 1
    return -1

def prev_token(src, i):
    j = i - 1
    while True:
        while j >= 0 and src[j].isspace():
            j -= 1
        if j < 0:
            return '', -1
        c = src[j]
        if c == ')':
            # skip a balanced (...) group (e.g. annotation args)
            depth = 0
            while j >= 0:
                if src[j] == ')': depth += 1
                elif src[j] == '(':
                    depth -= 1
                    if depth == 0:
                        j -= 1; break
                j -= 1
            continue
        if c.isalnum() or c in '_$.':
            e = j + 1
            while j >= 0 and (src[j].isalnum() or src[j] in '_$.'):
                j -= 1
            tok = src[j+1:e]
            # is this an annotation? (preceded by '@' with only whitespace between)
            k = j
            while k >= 0 and src[k].isspace():
                k -= 1
            if k >= 0 and src[k] == '@':
                # treat annotation name (and any dotted prefix) as transparent
                j = k - 1
                continue
            return tok, (j+1 if tok else -1)
        # boundary char
        return c, j

def parse_typeparams(block):
    # block is text inside <...>
    names = []
    for part in split_top_commas(block):
        part = part.strip()
        m = re.match(r'([A-Za-z_]\w*)', part)
        if m: names.append(m.group(1))
    return names

def split_top_commas(s):
    out = []
    depth = 0
    cur = []
    for ch in s:
        if ch == '<': depth += 1; cur.append(ch)
        elif ch == '>': depth -= 1; cur.append(ch)
        elif ch == ',' and depth == 0:
            out.append(''.join(cur)); cur = []
        else: cur.append(ch)
    if cur: out.append(''.join(cur))
    return out

def split_params(p):
    out = []
    depth = 0
    cur = []
    for ch in p:
        if ch in '([{<': depth += 1; cur.append(ch)
        elif ch in ')]}>': depth -= 1; cur.append(ch)
        elif ch == ',' and depth == 0:
            out.append(''.join(cur)); cur=[]
        else: cur.append(ch)
    if cur: out.append(''.join(cur))
    return [x.strip() for x in out if x.strip()]

def param_type(param):
    # remove annotations
    p = re.sub(r'@\w+(\([^)]*\))?\s*', '', param)
    p = p.replace('final ', '').strip()
    # handle varargs
    p = p.replace('...', '[]')
    # last identifier token is the name; type is the rest
    m = re.search(r'([A-Za-z_]\w*)\s*(\[\s*\])*\s*$', p)
    if not m:
        return p
    name = m.group(1)
    arr = m.group(2) or ''
    typepart = p[:m.start()].strip()
    return typepart + arr

def find_methods(clean_src):
    results = []
    i = 0
    n = len(clean_src)
    while i < n:
        if clean_src[i] != '<':
            i += 1; continue
        tok, tokpos = prev_token(clean_src, i)
        # must be preceded by a modifier or a member boundary
        if tok not in MODIFIERS and tok not in ('', '{', '}', ';', ')'):
            i += 1; continue
        close = match_angle(clean_src, i)
        if close == -1:
            i += 1; continue
        block = clean_src[i+1:close]
        # reject if block looks like an expression (operators, keywords that aren't bounds)
        if re.search(r'(==|!=|<=|>=|&&|\|\|| new | return |[+\-*/%])', block):
            i += 1; continue
        names = parse_typeparams(block)
        if not names:
            i += 1; continue
        # after close, expect returnType methodName(
        after = close + 1
        # find the opening paren of the parameter list
        # method name is last identifier before '('
        # scan forward to '(' at depth 0 (relative to generics in return type)
        j = after
        depth = 0
        paren = -1
        while j < n:
            ch = clean_src[j]
            if ch == '<': depth += 1
            elif ch == '>': depth -= 1
            elif ch == '(' and depth == 0:
                paren = j; break
            elif ch == ';' or ch == '{' or ch == '}':
                break
            j += 1
        if paren == -1:
            i += 1; continue
        sig = clean_src[after:paren]
        # method name = trailing identifier of sig
        mm = re.search(r'([A-Za-z_]\w*)\s*$', sig)
        if not mm:
            i += 1; continue
        mname = mm.group(1)
        rtype = sig[:mm.start()].strip()
        if not rtype:
            i += 1; continue
        # param list
        # find matching ')'
        pc = 1
        k = paren + 1
        while k < n and pc > 0:
            if clean_src[k] == '(': pc += 1
            elif clean_src[k] == ')': pc -= 1
            k += 1
        params_text = clean_src[paren+1:k-1]
        params = split_params(params_text)
        ptypes = [param_type(p) for p in params]
        # count usages
        region = rtype + ' ' + ' '.join(ptypes)
        counts = {}
        for nm in names:
            cnt = len(re.findall(r'\b' + re.escape(nm) + r'\b', region))
            counts[nm] = cnt
        results.append((mname, names, rtype, params, counts, sig, params_text))
        i = close + 1
    return results

def is_nested(region, name):
    m = re.search(r'\b' + re.escape(name) + r'\b', region)
    if not m: return None
    pos = m.start()
    depth = 0
    for ch in region[:pos]:
        if ch == '<': depth += 1
        elif ch == '>': depth = max(0, depth-1)
    return depth > 0

def main():
    hits = []
    for dp, _, fns in os.walk(ROOT):
        for fn in fns:
            if not fn.endswith('.java'): continue
            path = os.path.join(dp, fn)
            src = open(path, encoding='utf-8', errors='replace').read()
            cs = clean(src)
            for mname, names, rtype, params, counts, sig, ptxt in find_methods(cs):
                once = [nm for nm in names if counts[nm] == 1]
                zero = [nm for nm in names if counts[nm] == 0]
                if once or zero:
                    rel = os.path.relpath(path, ROOT)
                    region = rtype + ' ' + ' '.join(param_type(p) for p in params)
                    nested = {nm: is_nested(region, nm) for nm in (once+zero)}
                    hits.append((rel, mname, names, counts, rtype, ptxt, once, zero, nested))
    elig = 0
    for rel, mname, names, counts, rtype, ptxt, once, zero, nested in hits:
        flat = ' '.join(ptxt.split())
        tag = ','.join((f"{nm}:{'W' if nested.get(nm) else 'R'}") for nm in once) + ('|' + ','.join(f"{nm}:zero" for nm in zero) if zero else '')
        print(f"{rel}\t<{', '.join(names)}>\t{rtype} {mname}({flat})\t[{tag}]")
        elig += sum(1 for nm in once if nested.get(nm))
    print(f"\nTOTAL methods flagged: {len(hits)}  (wildcard-eligible single-use type params: {elig})")

if __name__ == "__main__":
    main()