"""Port of repairJSON_ from Apps Script — fixes unescaped quotes/newlines in model JSON."""


def repair_json_string(s: str) -> str:
    res = []
    in_string = False
    esc = False
    for i, c in enumerate(s):
        if esc:
            res.append(c)
            esc = False
            continue
        if c == "\\":
            esc = True
            res.append(c)
            continue
        if c == '"':
            if not in_string:
                in_string = True
                res.append(c)
            else:
                rest = s[i + 1 :].lstrip()
                if not rest or rest[0] in ",}]:":
                    in_string = False
                    res.append(c)
                else:
                    res.append('\\"')
            continue
        if in_string and c in "\n\r":
            continue
        res.append(c)
    out = "".join(res)
    # trailing commas before } or ]
    import re

    return re.sub(r",(\s*[\}\]])", r"\1", out)
