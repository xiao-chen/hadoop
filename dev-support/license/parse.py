import os
import sys
import re

def consume(line):
    if not line:
      return "", ""
    # Skip spaces
    line = consume_spaces(line)

    idx = 0
    if line[idx] == "(":
        # consume until a closing parens is found
        num_open = 0
        num_closed = 0
        while idx < len(line):
            if line[idx] == "(":
                num_open += 1
            elif line[idx] == ")":
                num_closed += 1
                if num_closed == num_open:
                    idx += 1
                    return line[:idx], line[idx:]
            idx += 1
        raise Exception("Could not consume parens token from line: " + line)
    else:
        # consume until EOL or an open parens is found
        while idx < len(line):
            if line[idx] == "(":
                break
            idx += 1
        return line[:idx], line[idx:]

    return "", ""

def consume_spaces(line):
    idx = 0
    while line[idx] == " " and idx < len(line):
        idx += 1
    return line[idx:]

lines = open("THIRD-PARTY.txt", "r").readlines()
for line in lines:
    line = line[:-1]
    licenses = []
    rest = line
    tok, rest = consume(rest)
    while tok.startswith("("):
        licenses += [tok[1:-1]]
        tok, rest = consume(rest)
    dep = tok
    tok, rest = consume(rest)
    info = tok[1:-1]
    print "\t".join([dep, ",".join(licenses), info])
