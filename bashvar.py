#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import logging
import tempfile
import warnings
import subprocess
import collections
import pyparsing as pp

re_variable = re.compile('^\\s*([a-zA-Z_][a-zA-Z0-9_]*)=')

whitespace = pp.White(ws=' \t').suppress().setName("whitespace")
optwhitespace = pp.Optional(whitespace).setName("optwhitespace")
comment = ('#' + pp.CharsNotIn('\n')).setName("comment")
integer = (pp.Word(pp.nums) | pp.Combine('-' + pp.Word(pp.nums)))

varname = pp.Word(pp.alphas + '_', pp.alphanums + '_').setResultsName("varname")
# ${parameter/pattern/string}
substsafe = pp.CharsNotIn('/#%*?[}\'"`\\')

expansion_param = pp.Group(
    pp.Literal('$').setResultsName("expansion") +
    # we don't want to parse all the expansions
    ((
        pp.Literal('{').suppress() +
        varname +
        pp.Optional(
            (pp.Literal(':').setResultsName("exptype") + (
                pp.Word(pp.nums) |
                (whitespace + pp.Combine('-' + pp.Word(pp.nums)))
                ).setResultsName("offset") +
                pp.Optional(pp.Literal(':') + integer.setResultsName("length")))
            ^ ((pp.Literal('//') ^ pp.Literal('/')).setResultsName("exptype") +
                pp.Optional(substsafe.setResultsName("pattern") +
                pp.Optional(pp.Literal('/') +
                pp.Optional(substsafe, '').setResultsName("string")))
            )
        ) +
        pp.Literal('}').suppress()
    ) | varname)
)

singlequote = pp.Group(
    pp.Literal("'").setResultsName("quote") +
    pp.Optional(pp.CharsNotIn("'"), '').setResultsName("value") +
    pp.Literal("'").suppress()
).setName("singlequote")
doublequote_escape = (
    (pp.Literal('\\').suppress() + pp.Word('$`"\\', exact=1)) |
    pp.Literal('\\\n').suppress()
)
doublequote = pp.Group(
    pp.Literal('"').setResultsName("quote") +
    pp.Group(pp.ZeroOrMore(
        doublequote_escape | expansion_param | pp.CharsNotIn('$`\\*"')
    )).setResultsName("value") +
    pp.Literal('"').suppress()
).setName("doublequote")

texttoken = (
    singlequote | doublequote | expansion_param |
    pp.CharsNotIn('~{}()$\'"`\\*?[] \t\n')
)
varvalue = pp.Group(pp.ZeroOrMore(texttoken)).setResultsName('varvalue')
varassign = (
    varname +
    (pp.Literal('=') | pp.Literal('+=')).setResultsName('operator') +
    varvalue
).setName('varassign').leaveWhitespace()

line = pp.Group(
    pp.lineStart + optwhitespace +
    pp.Optional(varassign) + optwhitespace +
    pp.Optional(comment).suppress() +
    pp.lineEnd.suppress()
).setName('line').leaveWhitespace()

bashvarfile = pp.ZeroOrMore(line)

ParseException = pp.ParseException

class VariableWarning(UserWarning):
    pass

class ParseError(Exception):
    pass

def combine_value(tokens, variables):
    val = ''
    if tokens.get('quote') == '"':
        val += combine_value(tokens['value'], variables)
    elif tokens.get('quote') == "'":
        val += tokens['value']
    elif tokens.get('expansion') == '$':
        varname = tokens['varname']
        if varname in variables:
            var = variables[varname]
            exptype = tokens.get('exptype')
            if exptype is None:
                pass
            elif exptype == ':':
                if 'offset' in tokens:
                    offset = int(tokens['offset'].strip())
                    if 'length' in tokens:
                        length = int(tokens['length'])
                        if length >= 0:
                            var = var[offset:offset+length]
                        else:
                            var = var[offset:length]
                    else:
                        var = var[offset:]
            elif exptype in '//':
                pattern = tokens.get('pattern', '')
                newstring = tokens.get('string', '')
                if exptype == '/':
                    var = var.replace(pattern, newstring, 1)
                else:
                    var = var.replace(pattern, newstring)
            val += var
        else:
            warnings.warn('variable "%s" is undefined' % varname, VariableWarning)
    else:
        for tok in tokens:
            if isinstance(tok, str):
                val += tok
            else:
                val += combine_value(tok, variables)
    return ''.join(val)

def eval_bashvar_literal(source):
    parsed = bashvarfile.parseString(source, parseAll=True)
    variables = collections.OrderedDict()
    for line in parsed:
        if not line:
            continue
        val = combine_value(line['varvalue'], variables)
        if line['operator'] == '=':
            variables[line['varname']] = val
        elif line['operator'] == '+=':
            if line['varname'] in variables:
                variables[line['varname']] += val
            else:
                warnings.warn(
                    'variable "%s" is undefined' % line['varname'], VariableWarning)
                variables[line['varname']] = val
    return variables

def uniq(seq):  # Dave Kirby
    # Order preserving
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]

def eval_bashvar_ext(source, filename=None):
    # we don't specify encoding here because the env will do.
    var = []
    stdin = []
    for ln in source.splitlines(True):
        match = re_variable.match(ln)
        if match:
            var.append(match.group(1))
        stdin.append(ln)
    stdin.append('\n')
    var = uniq(var)
    for v in var:
        # workaround variables containing newlines
        stdin.append('echo "${%s//$\'\\n\'/\\\\n}"\n' % v)
    with tempfile.TemporaryDirectory() as tmpdir:
        outs, errs = subprocess.Popen(
            ('bash',), cwd=tmpdir,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate(''.join(stdin).encode('utf-8'))
    if errs:
        logging.warning('%s: %s', filename, errs.decode('utf-8', 'backslashreplace').rstrip())
    lines = [l.replace('\\n', '\n') for l in outs.decode('utf-8').splitlines()]
    if len(var) != len(lines):
        logging.error('%s: bash output not expected', filename)
    return collections.OrderedDict(zip(var, lines))

def eval_bashvar(source, filename=None):
    try:
        with warnings.catch_warnings(record=True) as wns:
            ret = eval_bashvar_literal(source)
            for w in wns:
                if issubclass(w.category, VariableWarning):
                    logging.warning('%s: %s', filename, w.message)
            return ret
    except pp.ParseException:
        return eval_bashvar_ext(source, filename)

def read_bashvar(fp, filename=None):
    return eval_bashvar(fp.read(), filename or getattr(fp, 'name', None))
