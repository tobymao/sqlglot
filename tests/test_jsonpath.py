import json
import os
import unittest

from sqlglot import exp, jsonpath
from sqlglot.errors import ParseError, TokenError
from tests.helpers import FIXTURES_DIR


class TestJsonpath(unittest.TestCase):
    maxDiff = None

    def test_jsonpath(self):
        expected_expressions = [
            exp.JSONPathRoot(),
            exp.JSONPathKey(this=exp.JSONPathWildcard()),
            exp.JSONPathKey(this="a"),
            exp.JSONPathSubscript(this=0),
            exp.JSONPathKey(this="x"),
            exp.JSONPathUnion(expressions=[exp.JSONPathWildcard(), "y", 1]),
            exp.JSONPathKey(this="z"),
            exp.JSONPathSelector(this=exp.JSONPathFilter(this="(@.a == 'b'), 1:")),
            exp.JSONPathSubscript(this=exp.JSONPathSlice(start=1, end=5, step=None)),
            exp.JSONPathUnion(expressions=[1, exp.JSONPathFilter(this="@.a")]),
            exp.JSONPathSelector(this=exp.JSONPathScript(this="@.x)")),
        ]
        self.assertEqual(
            jsonpath.parse("$.*.a[0]['x'][*, 'y', 1].z[?(@.a == 'b'), 1:][1:5][1,?@.a][(@.x)]"),
            exp.JSONPath(expressions=expected_expressions),
        )

    def test_identity(self):
        for selector, expected in (
            ("$.select", "$.select"),
            ("$[(@.length-1)]", "$[(@.length-1)]"),
            ("$[((@.length-1))]", "$[((@.length-1))]"),
        ):
            with self.subTest(f"{selector} -> {expected}"):
                self.assertEqual(jsonpath.parse(selector).sql(), f"'{expected}'")

    def test_cts_file(self):
        with open(os.path.join(FIXTURES_DIR, "jsonpath", "cts.json")) as file:
            tests = json.load(file)["tests"]

        # sqlglot json path generator rewrites to a normal form
        overrides = {
            "$.☺": '$["☺"]',
            """$['a',1]""": """$["a",1]""",
            """$[*,'a']""": """$[*,"a"]""",
            """$..['a','d']""": """$..["a","d"]""",
            """$[1, ?@.a=='b', 1:]""": """$[1,?@.a=='b', 1:]""",
            """$["a"]""": """$.a""",
            """$["c"]""": """$.c""",
            """$['a']""": """$.a""",
            """$['c']""": """$.c""",
            """$[' ']""": """$[" "]""",
            """$['\\'']""": """$["\'"]""",
            """$['\\\\']""": """$["\\\\"]""",
            """$['\\/']""": """$["\\/"]""",
            """$['\\b']""": """$["\\b"]""",
            """$['\\f']""": """$["\\f"]""",
            """$['\\n']""": """$["\\n"]""",
            """$['\\r']""": """$["\\r"]""",
            """$['\\t']""": """$["\\t"]""",
            """$['\\u263A']""": """$["\\u263A"]""",
            """$['\\u263a']""": """$["\\u263a"]""",
            """$['\\uD834\\uDD1E']""": """$["\\uD834\\uDD1E"]""",
            """$['\\uD83D\\uDE00']""": """$["\\uD83D\\uDE00"]""",
            """$['']""": """$[""]""",
            """$[? @.a]""": """$[?@.a]""",
            """$[?\n@.a]""": """$[?@.a]""",
            """$[?\t@.a]""": """$[?@.a]""",
            """$[?\r@.a]""": """$[?@.a]""",
            """$[? (@.a)]""": """$[?(@.a)]""",
            """$[?\n(@.a)]""": """$[?(@.a)]""",
            """$[?\t(@.a)]""": """$[?(@.a)]""",
            """$[?\r(@.a)]""": """$[?(@.a)]""",
            """$[ ?@.a]""": """$[?@.a]""",
            """$[\n?@.a]""": """$[?@.a]""",
            """$[\t?@.a]""": """$[?@.a]""",
            """$[\r?@.a]""": """$[?@.a]""",
            """$ ['a']""": """$.a""",
            """$\n['a']""": """$.a""",
            """$\t['a']""": """$.a""",
            """$\r['a']""": """$.a""",
            """$['a'] ['b']""": """$.a.b""",
            """$['a'] \n['b']""": """$.a.b""",
            """$['a'] \t['b']""": """$.a.b""",
            """$['a'] \r['b']""": """$.a.b""",
            """$ .a""": """$.a""",
            """$\n.a""": """$.a""",
            """$\t.a""": """$.a""",
            """$\r.a""": """$.a""",
            """$[ 'a']""": """$.a""",
            """$[\n'a']""": """$.a""",
            """$[\t'a']""": """$.a""",
            """$[\r'a']""": """$.a""",
            """$['a' ]""": """$.a""",
            """$['a'\n]""": """$.a""",
            """$['a'\t]""": """$.a""",
            """$['a'\r]""": """$.a""",
            """$['a' ,'b']""": """$["a","b"]""",
            """$['a'\n,'b']""": """$["a","b"]""",
            """$['a'\t,'b']""": """$["a","b"]""",
            """$['a'\r,'b']""": """$["a","b"]""",
            """$['a', 'b']""": """$["a","b"]""",
            """$['a',\n'b']""": """$["a","b"]""",
            """$['a',\t'b']""": """$["a","b"]""",
            """$['a',\r'b']""": """$["a","b"]""",
            """$[1 :5:2]""": """$[1:5:2]""",
            """$[1\n:5:2]""": """$[1:5:2]""",
            """$[1\t:5:2]""": """$[1:5:2]""",
            """$[1\r:5:2]""": """$[1:5:2]""",
            """$[1: 5:2]""": """$[1:5:2]""",
            """$[1:\n5:2]""": """$[1:5:2]""",
            """$[1:\t5:2]""": """$[1:5:2]""",
            """$[1:\r5:2]""": """$[1:5:2]""",
            """$[1:5 :2]""": """$[1:5:2]""",
            """$[1:5\n:2]""": """$[1:5:2]""",
            """$[1:5\t:2]""": """$[1:5:2]""",
            """$[1:5\r:2]""": """$[1:5:2]""",
            """$[1:5: 2]""": """$[1:5:2]""",
            """$[1:5:\n2]""": """$[1:5:2]""",
            """$[1:5:\t2]""": """$[1:5:2]""",
            """$[1:5:\r2]""": """$[1:5:2]""",
        }

        for test in tests:
            selector = test["selector"]

            with self.subTest(f"{selector.strip()} /* {test['name']} */"):
                if test.get("invalid_selector"):
                    try:
                        jsonpath.parse(selector)
                    except (ParseError, TokenError):
                        pass
                else:
                    path = jsonpath.parse(selector)
                    self.assertEqual(path.sql(), f"'{overrides.get(selector, selector)}'")
