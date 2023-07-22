from __future__ import annotations

import logging
import typing as t
from collections import defaultdict

from sqlglot import exp
from sqlglot.errors import ErrorLevel, ParseError, concat_messages, merge_errors
from sqlglot.helper import apply_index_offset, ensure_list, seq_get
from sqlglot.time import format_time
from sqlglot.tokens import Token, Tokenizer, TokenType
from sqlglot.trie import TrieResult, in_trie, new_trie

if t.TYPE_CHECKING:
    from sqlglot._typing import E

logger = logging.getLogger("sqlglot")


def parse_var_map(args: t.List) -> exp.StarMap | exp.VarMap:
    if len(args) == 1 and args[0].is_star:
        return exp.StarMap(this=args[0])

    keys = []
    values = []
    for i in range(0, len(args), 2):
        keys.append(args[i])
        values.append(args[i + 1])

    return exp.VarMap(
        keys=exp.Array(expressions=keys),
        values=exp.Array(expressions=values),
    )


def parse_like(args: t.List) -> exp.Escape | exp.Like:
    like = exp.Like(this=seq_get(args, 1), expression=seq_get(args, 0))
    return exp.Escape(this=like, expression=seq_get(args, 2)) if len(args) > 2 else like


def binary_range_parser(
    expr_type: t.Type[exp.Expression],
) -> t.Callable[[Parser, t.Optional[exp.Expression]], t.Optional[exp.Expression]]:
    return lambda self, this: self._parse_escape(
        self.expression(expr_type, this=this, expression=self._parse_bitwise())
    )


class _Parser(type):
    def __new__(cls, clsname, bases, attrs):
        klass = super().__new__(cls, clsname, bases, attrs)

        klass.SHOW_TRIE = new_trie(key.split(" ") for key in klass.SHOW_PARSERS)
        klass.SET_TRIE = new_trie(key.split(" ") for key in klass.SET_PARSERS)

        return klass


class Parser(metaclass=_Parser):
    """
    Parser consumes a list of tokens produced by the Tokenizer and produces a parsed syntax tree.

    Args:
        error_level: The desired error level.
            Default: ErrorLevel.IMMEDIATE
        error_message_context: Determines the amount of context to capture from a
            query string when displaying the error message (in number of characters).
            Default: 100
        max_errors: Maximum number of error messages to include in a raised ParseError.
            This is only relevant if error_level is ErrorLevel.RAISE.
            Default: 3
    """

    FUNCTIONS: t.Dict[str, t.Callable] = {
        **{name: f.from_arg_list for f in exp.ALL_FUNCTIONS for name in f.sql_names()},
        "DATE_TO_DATE_STR": lambda args: exp.Cast(
            this=seq_get(args, 0),
            to=exp.DataType(this=exp.DataType.Type.TEXT),
        ),
        "GLOB": lambda args: exp.Glob(this=seq_get(args, 1), expression=seq_get(args, 0)),
        "LIKE": parse_like,
        "TIME_TO_TIME_STR": lambda args: exp.Cast(
            this=seq_get(args, 0),
            to=exp.DataType(this=exp.DataType.Type.TEXT),
        ),
        "TS_OR_DS_TO_DATE_STR": lambda args: exp.Substring(
            this=exp.Cast(
                this=seq_get(args, 0),
                to=exp.DataType(this=exp.DataType.Type.TEXT),
            ),
            start=exp.Literal.number(1),
            length=exp.Literal.number(10),
        ),
        "VAR_MAP": parse_var_map,
    }

    NO_PAREN_FUNCTIONS = {
        TokenType.CURRENT_DATE: exp.CurrentDate,
        TokenType.CURRENT_DATETIME: exp.CurrentDate,
        TokenType.CURRENT_TIME: exp.CurrentTime,
        TokenType.CURRENT_TIMESTAMP: exp.CurrentTimestamp,
        TokenType.CURRENT_USER: exp.CurrentUser,
    }

    NESTED_TYPE_TOKENS = {
        TokenType.ARRAY,
        TokenType.MAP,
        TokenType.NULLABLE,
        TokenType.STRUCT,
    }

    ENUM_TYPE_TOKENS = {
        TokenType.ENUM,
    }

    TYPE_TOKENS = {
        TokenType.BIT,
        TokenType.BOOLEAN,
        TokenType.TINYINT,
        TokenType.UTINYINT,
        TokenType.SMALLINT,
        TokenType.USMALLINT,
        TokenType.INT,
        TokenType.UINT,
        TokenType.BIGINT,
        TokenType.UBIGINT,
        TokenType.INT128,
        TokenType.UINT128,
        TokenType.INT256,
        TokenType.UINT256,
        TokenType.FLOAT,
        TokenType.DOUBLE,
        TokenType.CHAR,
        TokenType.NCHAR,
        TokenType.VARCHAR,
        TokenType.NVARCHAR,
        TokenType.TEXT,
        TokenType.MEDIUMTEXT,
        TokenType.LONGTEXT,
        TokenType.MEDIUMBLOB,
        TokenType.LONGBLOB,
        TokenType.BINARY,
        TokenType.VARBINARY,
        TokenType.JSON,
        TokenType.JSONB,
        TokenType.INTERVAL,
        TokenType.TIME,
        TokenType.TIMESTAMP,
        TokenType.TIMESTAMPTZ,
        TokenType.TIMESTAMPLTZ,
        TokenType.DATETIME,
        TokenType.DATETIME64,
        TokenType.DATE,
        TokenType.INT4RANGE,
        TokenType.INT4MULTIRANGE,
        TokenType.INT8RANGE,
        TokenType.INT8MULTIRANGE,
        TokenType.NUMRANGE,
        TokenType.NUMMULTIRANGE,
        TokenType.TSRANGE,
        TokenType.TSMULTIRANGE,
        TokenType.TSTZRANGE,
        TokenType.TSTZMULTIRANGE,
        TokenType.DATERANGE,
        TokenType.DATEMULTIRANGE,
        TokenType.DECIMAL,
        TokenType.BIGDECIMAL,
        TokenType.UUID,
        TokenType.GEOGRAPHY,
        TokenType.GEOMETRY,
        TokenType.HLLSKETCH,
        TokenType.HSTORE,
        TokenType.PSEUDO_TYPE,
        TokenType.SUPER,
        TokenType.SERIAL,
        TokenType.SMALLSERIAL,
        TokenType.BIGSERIAL,
        TokenType.XML,
        TokenType.UNIQUEIDENTIFIER,
        TokenType.USERDEFINED,
        TokenType.MONEY,
        TokenType.SMALLMONEY,
        TokenType.ROWVERSION,
        TokenType.IMAGE,
        TokenType.VARIANT,
        TokenType.OBJECT,
        TokenType.INET,
        TokenType.ENUM,
        *NESTED_TYPE_TOKENS,
    }

    SUBQUERY_PREDICATES = {
        TokenType.ANY: exp.Any,
        TokenType.ALL: exp.All,
        TokenType.EXISTS: exp.Exists,
        TokenType.SOME: exp.Any,
    }

    RESERVED_KEYWORDS = {
        *Tokenizer.SINGLE_TOKENS.values(),
        TokenType.SELECT,
    }

    DB_CREATABLES = {
        TokenType.DATABASE,
        TokenType.SCHEMA,
        TokenType.TABLE,
        TokenType.VIEW,
        TokenType.DICTIONARY,
    }

    CREATABLES = {
        TokenType.COLUMN,
        TokenType.FUNCTION,
        TokenType.INDEX,
        TokenType.PROCEDURE,
        *DB_CREATABLES,
    }

    # Tokens that can represent identifiers
    ID_VAR_TOKENS = {
        TokenType.VAR,
        TokenType.ANTI,
        TokenType.APPLY,
        TokenType.ASC,
        TokenType.AUTO_INCREMENT,
        TokenType.BEGIN,
        TokenType.CACHE,
        TokenType.CASE,
        TokenType.COLLATE,
        TokenType.COMMAND,
        TokenType.COMMENT,
        TokenType.COMMIT,
        TokenType.CONSTRAINT,
        TokenType.DEFAULT,
        TokenType.DELETE,
        TokenType.DESC,
        TokenType.DESCRIBE,
        TokenType.DICTIONARY,
        TokenType.DIV,
        TokenType.END,
        TokenType.EXECUTE,
        TokenType.ESCAPE,
        TokenType.FALSE,
        TokenType.FIRST,
        TokenType.FILTER,
        TokenType.FORMAT,
        TokenType.FULL,
        TokenType.IF,
        TokenType.IS,
        TokenType.ISNULL,
        TokenType.INTERVAL,
        TokenType.KEEP,
        TokenType.LEFT,
        TokenType.LOAD,
        TokenType.MERGE,
        TokenType.NATURAL,
        TokenType.NEXT,
        TokenType.OFFSET,
        TokenType.ORDINALITY,
        TokenType.OVERWRITE,
        TokenType.PARTITION,
        TokenType.PERCENT,
        TokenType.PIVOT,
        TokenType.PRAGMA,
        TokenType.RANGE,
        TokenType.REFERENCES,
        TokenType.RIGHT,
        TokenType.ROW,
        TokenType.ROWS,
        TokenType.SEMI,
        TokenType.SET,
        TokenType.SETTINGS,
        TokenType.SHOW,
        TokenType.TEMPORARY,
        TokenType.TOP,
        TokenType.TRUE,
        TokenType.UNIQUE,
        TokenType.UNPIVOT,
        TokenType.UPDATE,
        TokenType.VOLATILE,
        TokenType.WINDOW,
        *CREATABLES,
        *SUBQUERY_PREDICATES,
        *TYPE_TOKENS,
        *NO_PAREN_FUNCTIONS,
    }

    INTERVAL_VARS = ID_VAR_TOKENS - {TokenType.END}

    TABLE_ALIAS_TOKENS = ID_VAR_TOKENS - {
        TokenType.APPLY,
        TokenType.ASOF,
        TokenType.FULL,
        TokenType.LEFT,
        TokenType.LOCK,
        TokenType.NATURAL,
        TokenType.OFFSET,
        TokenType.RIGHT,
        TokenType.WINDOW,
    }

    COMMENT_TABLE_ALIAS_TOKENS = TABLE_ALIAS_TOKENS - {TokenType.IS}

    UPDATE_ALIAS_TOKENS = TABLE_ALIAS_TOKENS - {TokenType.SET}

    TRIM_TYPES = {"LEADING", "TRAILING", "BOTH"}

    FUNC_TOKENS = {
        TokenType.COMMAND,
        TokenType.CURRENT_DATE,
        TokenType.CURRENT_DATETIME,
        TokenType.CURRENT_TIMESTAMP,
        TokenType.CURRENT_TIME,
        TokenType.CURRENT_USER,
        TokenType.FILTER,
        TokenType.FIRST,
        TokenType.FORMAT,
        TokenType.GLOB,
        TokenType.IDENTIFIER,
        TokenType.INDEX,
        TokenType.ISNULL,
        TokenType.ILIKE,
        TokenType.LIKE,
        TokenType.MERGE,
        TokenType.OFFSET,
        TokenType.PRIMARY_KEY,
        TokenType.RANGE,
        TokenType.REPLACE,
        TokenType.RLIKE,
        TokenType.ROW,
        TokenType.UNNEST,
        TokenType.VAR,
        TokenType.LEFT,
        TokenType.RIGHT,
        TokenType.DATE,
        TokenType.DATETIME,
        TokenType.TABLE,
        TokenType.TIMESTAMP,
        TokenType.TIMESTAMPTZ,
        TokenType.WINDOW,
        TokenType.XOR,
        *TYPE_TOKENS,
        *SUBQUERY_PREDICATES,
    }

    CONJUNCTION = {
        TokenType.AND: exp.And,
        TokenType.OR: exp.Or,
    }

    EQUALITY = {
        TokenType.EQ: exp.EQ,
        TokenType.NEQ: exp.NEQ,
        TokenType.NULLSAFE_EQ: exp.NullSafeEQ,
    }

    COMPARISON = {
        TokenType.GT: exp.GT,
        TokenType.GTE: exp.GTE,
        TokenType.LT: exp.LT,
        TokenType.LTE: exp.LTE,
    }

    BITWISE = {
        TokenType.AMP: exp.BitwiseAnd,
        TokenType.CARET: exp.BitwiseXor,
        TokenType.PIPE: exp.BitwiseOr,
        TokenType.DPIPE: exp.DPipe,
    }

    TERM = {
        TokenType.DASH: exp.Sub,
        TokenType.PLUS: exp.Add,
        TokenType.MOD: exp.Mod,
        TokenType.COLLATE: exp.Collate,
    }

    FACTOR = {
        TokenType.DIV: exp.IntDiv,
        TokenType.LR_ARROW: exp.Distance,
        TokenType.SLASH: exp.Div,
        TokenType.STAR: exp.Mul,
    }

    TIMESTAMPS = {
        TokenType.TIME,
        TokenType.TIMESTAMP,
        TokenType.TIMESTAMPTZ,
        TokenType.TIMESTAMPLTZ,
    }

    SET_OPERATIONS = {
        TokenType.UNION,
        TokenType.INTERSECT,
        TokenType.EXCEPT,
    }

    JOIN_METHODS = {
        TokenType.NATURAL,
        TokenType.ASOF,
    }

    JOIN_SIDES = {
        TokenType.LEFT,
        TokenType.RIGHT,
        TokenType.FULL,
    }

    JOIN_KINDS = {
        TokenType.INNER,
        TokenType.OUTER,
        TokenType.CROSS,
        TokenType.SEMI,
        TokenType.ANTI,
    }

    JOIN_HINTS: t.Set[str] = set()

    LAMBDAS = {
        TokenType.ARROW: lambda self, expressions: self.expression(
            exp.Lambda,
            this=self._replace_lambda(
                self._parse_conjunction(),
                {node.name for node in expressions},
            ),
            expressions=expressions,
        ),
        TokenType.FARROW: lambda self, expressions: self.expression(
            exp.Kwarg,
            this=exp.var(expressions[0].name),
            expression=self._parse_conjunction(),
        ),
    }

    COLUMN_OPERATORS = {
        TokenType.DOT: None,
        TokenType.DCOLON: lambda self, this, to: self.expression(
            exp.Cast if self.STRICT_CAST else exp.TryCast,
            this=this,
            to=to,
        ),
        TokenType.ARROW: lambda self, this, path: self.expression(
            exp.JSONExtract,
            this=this,
            expression=path,
        ),
        TokenType.DARROW: lambda self, this, path: self.expression(
            exp.JSONExtractScalar,
            this=this,
            expression=path,
        ),
        TokenType.HASH_ARROW: lambda self, this, path: self.expression(
            exp.JSONBExtract,
            this=this,
            expression=path,
        ),
        TokenType.DHASH_ARROW: lambda self, this, path: self.expression(
            exp.JSONBExtractScalar,
            this=this,
            expression=path,
        ),
        TokenType.PLACEHOLDER: lambda self, this, key: self.expression(
            exp.JSONBContains,
            this=this,
            expression=key,
        ),
    }

    EXPRESSION_PARSERS = {
        exp.Cluster: lambda self: self._parse_sort(exp.Cluster, TokenType.CLUSTER_BY),
        exp.Column: lambda self: self._parse_column(),
        exp.Condition: lambda self: self._parse_conjunction(),
        exp.DataType: lambda self: self._parse_types(),
        exp.Expression: lambda self: self._parse_statement(),
        exp.From: lambda self: self._parse_from(),
        exp.Group: lambda self: self._parse_group(),
        exp.Having: lambda self: self._parse_having(),
        exp.Identifier: lambda self: self._parse_id_var(),
        exp.Join: lambda self: self._parse_join(),
        exp.Lambda: lambda self: self._parse_lambda(),
        exp.Lateral: lambda self: self._parse_lateral(),
        exp.Limit: lambda self: self._parse_limit(),
        exp.Offset: lambda self: self._parse_offset(),
        exp.Order: lambda self: self._parse_order(),
        exp.Ordered: lambda self: self._parse_ordered(),
        exp.Properties: lambda self: self._parse_properties(),
        exp.Qualify: lambda self: self._parse_qualify(),
        exp.Returning: lambda self: self._parse_returning(),
        exp.Sort: lambda self: self._parse_sort(exp.Sort, TokenType.SORT_BY),
        exp.Table: lambda self: self._parse_table_parts(),
        exp.TableAlias: lambda self: self._parse_table_alias(),
        exp.Where: lambda self: self._parse_where(),
        exp.Window: lambda self: self._parse_named_window(),
        exp.With: lambda self: self._parse_with(),
        "JOIN_TYPE": lambda self: self._parse_join_parts(),
    }

    STATEMENT_PARSERS = {
        TokenType.ALTER: lambda self: self._parse_alter(),
        TokenType.BEGIN: lambda self: self._parse_transaction(),
        TokenType.CACHE: lambda self: self._parse_cache(),
        TokenType.COMMIT: lambda self: self._parse_commit_or_rollback(),
        TokenType.COMMENT: lambda self: self._parse_comment(),
        TokenType.CREATE: lambda self: self._parse_create(),
        TokenType.DELETE: lambda self: self._parse_delete(),
        TokenType.DESC: lambda self: self._parse_describe(),
        TokenType.DESCRIBE: lambda self: self._parse_describe(),
        TokenType.DROP: lambda self: self._parse_drop(),
        TokenType.FROM: lambda self: exp.select("*").from_(
            t.cast(exp.From, self._parse_from(skip_from_token=True))
        ),
        TokenType.INSERT: lambda self: self._parse_insert(),
        TokenType.LOAD: lambda self: self._parse_load(),
        TokenType.MERGE: lambda self: self._parse_merge(),
        TokenType.PIVOT: lambda self: self._parse_simplified_pivot(),
        TokenType.PRAGMA: lambda self: self.expression(exp.Pragma, this=self._parse_expression()),
        TokenType.ROLLBACK: lambda self: self._parse_commit_or_rollback(),
        TokenType.SET: lambda self: self._parse_set(),
        TokenType.UNCACHE: lambda self: self._parse_uncache(),
        TokenType.UPDATE: lambda self: self._parse_update(),
        TokenType.USE: lambda self: self.expression(
            exp.Use,
            kind=self._match_texts(("ROLE", "WAREHOUSE", "DATABASE", "SCHEMA"))
            and exp.var(self._prev.text),
            this=self._parse_table(schema=False),
        ),
    }

    UNARY_PARSERS = {
        TokenType.PLUS: lambda self: self._parse_unary(),  # Unary + is handled as a no-op
        TokenType.NOT: lambda self: self.expression(exp.Not, this=self._parse_equality()),
        TokenType.TILDA: lambda self: self.expression(exp.BitwiseNot, this=self._parse_unary()),
        TokenType.DASH: lambda self: self.expression(exp.Neg, this=self._parse_unary()),
    }

    PRIMARY_PARSERS = {
        TokenType.STRING: lambda self, token: self.expression(
            exp.Literal, this=token.text, is_string=True
        ),
        TokenType.NUMBER: lambda self, token: self.expression(
            exp.Literal, this=token.text, is_string=False
        ),
        TokenType.STAR: lambda self, _: self.expression(
            exp.Star, **{"except": self._parse_except(), "replace": self._parse_replace()}
        ),
        TokenType.NULL: lambda self, _: self.expression(exp.Null),
        TokenType.TRUE: lambda self, _: self.expression(exp.Boolean, this=True),
        TokenType.FALSE: lambda self, _: self.expression(exp.Boolean, this=False),
        TokenType.BIT_STRING: lambda self, token: self.expression(exp.BitString, this=token.text),
        TokenType.HEX_STRING: lambda self, token: self.expression(exp.HexString, this=token.text),
        TokenType.BYTE_STRING: lambda self, token: self.expression(exp.ByteString, this=token.text),
        TokenType.INTRODUCER: lambda self, token: self._parse_introducer(token),
        TokenType.NATIONAL_STRING: lambda self, token: self.expression(
            exp.National, this=token.text
        ),
        TokenType.RAW_STRING: lambda self, token: self.expression(exp.RawString, this=token.text),
        TokenType.SESSION_PARAMETER: lambda self, _: self._parse_session_parameter(),
    }

    PLACEHOLDER_PARSERS = {
        TokenType.PLACEHOLDER: lambda self: self.expression(exp.Placeholder),
        TokenType.PARAMETER: lambda self: self._parse_parameter(),
        TokenType.COLON: lambda self: self.expression(exp.Placeholder, this=self._prev.text)
        if self._match_set((TokenType.NUMBER, TokenType.VAR))
        else None,
    }

    RANGE_PARSERS = {
        TokenType.BETWEEN: lambda self, this: self._parse_between(this),
        TokenType.GLOB: binary_range_parser(exp.Glob),
        TokenType.ILIKE: binary_range_parser(exp.ILike),
        TokenType.IN: lambda self, this: self._parse_in(this),
        TokenType.IRLIKE: binary_range_parser(exp.RegexpILike),
        TokenType.IS: lambda self, this: self._parse_is(this),
        TokenType.LIKE: binary_range_parser(exp.Like),
        TokenType.OVERLAPS: binary_range_parser(exp.Overlaps),
        TokenType.RLIKE: binary_range_parser(exp.RegexpLike),
        TokenType.SIMILAR_TO: binary_range_parser(exp.SimilarTo),
    }

    PROPERTY_PARSERS: t.Dict[str, t.Callable] = {
        "ALGORITHM": lambda self: self._parse_property_assignment(exp.AlgorithmProperty),
        "AUTO_INCREMENT": lambda self: self._parse_property_assignment(exp.AutoIncrementProperty),
        "BLOCKCOMPRESSION": lambda self: self._parse_blockcompression(),
        "CHARACTER SET": lambda self: self._parse_character_set(),
        "CHECKSUM": lambda self: self._parse_checksum(),
        "CLUSTER BY": lambda self: self._parse_cluster(),
        "CLUSTERED": lambda self: self._parse_clustered_by(),
        "COLLATE": lambda self: self._parse_property_assignment(exp.CollateProperty),
        "COMMENT": lambda self: self._parse_property_assignment(exp.SchemaCommentProperty),
        "COPY": lambda self: self._parse_copy_property(),
        "DATABLOCKSIZE": lambda self, **kwargs: self._parse_datablocksize(**kwargs),
        "DEFINER": lambda self: self._parse_definer(),
        "DETERMINISTIC": lambda self: self.expression(
            exp.StabilityProperty, this=exp.Literal.string("IMMUTABLE")
        ),
        "DISTKEY": lambda self: self._parse_distkey(),
        "DISTSTYLE": lambda self: self._parse_property_assignment(exp.DistStyleProperty),
        "ENGINE": lambda self: self._parse_property_assignment(exp.EngineProperty),
        "EXECUTE": lambda self: self._parse_property_assignment(exp.ExecuteAsProperty),
        "EXTERNAL": lambda self: self.expression(exp.ExternalProperty),
        "FALLBACK": lambda self, **kwargs: self._parse_fallback(**kwargs),
        "FORMAT": lambda self: self._parse_property_assignment(exp.FileFormatProperty),
        "FREESPACE": lambda self: self._parse_freespace(),
        "IMMUTABLE": lambda self: self.expression(
            exp.StabilityProperty, this=exp.Literal.string("IMMUTABLE")
        ),
        "JOURNAL": lambda self, **kwargs: self._parse_journal(**kwargs),
        "LANGUAGE": lambda self: self._parse_property_assignment(exp.LanguageProperty),
        "LAYOUT": lambda self: self._parse_dict_property(this="LAYOUT"),
        "LIFETIME": lambda self: self._parse_dict_range(this="LIFETIME"),
        "LIKE": lambda self: self._parse_create_like(),
        "LOCATION": lambda self: self._parse_property_assignment(exp.LocationProperty),
        "LOCK": lambda self: self._parse_locking(),
        "LOCKING": lambda self: self._parse_locking(),
        "LOG": lambda self, **kwargs: self._parse_log(**kwargs),
        "MATERIALIZED": lambda self: self.expression(exp.MaterializedProperty),
        "MERGEBLOCKRATIO": lambda self, **kwargs: self._parse_mergeblockratio(**kwargs),
        "MULTISET": lambda self: self.expression(exp.SetProperty, multi=True),
        "NO": lambda self: self._parse_no_property(),
        "ON": lambda self: self._parse_on_property(),
        "ORDER BY": lambda self: self._parse_order(skip_order_token=True),
        "PARTITION BY": lambda self: self._parse_partitioned_by(),
        "PARTITIONED BY": lambda self: self._parse_partitioned_by(),
        "PARTITIONED_BY": lambda self: self._parse_partitioned_by(),
        "PRIMARY KEY": lambda self: self._parse_primary_key(in_props=True),
        "RANGE": lambda self: self._parse_dict_range(this="RANGE"),
        "RETURNS": lambda self: self._parse_returns(),
        "ROW": lambda self: self._parse_row(),
        "ROW_FORMAT": lambda self: self._parse_property_assignment(exp.RowFormatProperty),
        "SET": lambda self: self.expression(exp.SetProperty, multi=False),
        "SETTINGS": lambda self: self.expression(
            exp.SettingsProperty, expressions=self._parse_csv(self._parse_set_item)
        ),
        "SORTKEY": lambda self: self._parse_sortkey(),
        "SOURCE": lambda self: self._parse_dict_property(this="SOURCE"),
        "STABLE": lambda self: self.expression(
            exp.StabilityProperty, this=exp.Literal.string("STABLE")
        ),
        "STORED": lambda self: self._parse_stored(),
        "TBLPROPERTIES": lambda self: self._parse_wrapped_csv(self._parse_property),
        "TEMP": lambda self: self.expression(exp.TemporaryProperty),
        "TEMPORARY": lambda self: self.expression(exp.TemporaryProperty),
        "TO": lambda self: self._parse_to_table(),
        "TRANSIENT": lambda self: self.expression(exp.TransientProperty),
        "TTL": lambda self: self._parse_ttl(),
        "USING": lambda self: self._parse_property_assignment(exp.FileFormatProperty),
        "VOLATILE": lambda self: self._parse_volatile_property(),
        "WITH": lambda self: self._parse_with_property(),
    }

    CONSTRAINT_PARSERS = {
        "AUTOINCREMENT": lambda self: self._parse_auto_increment(),
        "AUTO_INCREMENT": lambda self: self._parse_auto_increment(),
        "CASESPECIFIC": lambda self: self.expression(exp.CaseSpecificColumnConstraint, not_=False),
        "CHARACTER SET": lambda self: self.expression(
            exp.CharacterSetColumnConstraint, this=self._parse_var_or_string()
        ),
        "CHECK": lambda self: self.expression(
            exp.CheckColumnConstraint, this=self._parse_wrapped(self._parse_conjunction)
        ),
        "COLLATE": lambda self: self.expression(
            exp.CollateColumnConstraint, this=self._parse_var()
        ),
        "COMMENT": lambda self: self.expression(
            exp.CommentColumnConstraint, this=self._parse_string()
        ),
        "COMPRESS": lambda self: self._parse_compress(),
        "DEFAULT": lambda self: self.expression(
            exp.DefaultColumnConstraint, this=self._parse_bitwise()
        ),
        "ENCODE": lambda self: self.expression(exp.EncodeColumnConstraint, this=self._parse_var()),
        "FOREIGN KEY": lambda self: self._parse_foreign_key(),
        "FORMAT": lambda self: self.expression(
            exp.DateFormatColumnConstraint, this=self._parse_var_or_string()
        ),
        "GENERATED": lambda self: self._parse_generated_as_identity(),
        "IDENTITY": lambda self: self._parse_auto_increment(),
        "INLINE": lambda self: self._parse_inline(),
        "LIKE": lambda self: self._parse_create_like(),
        "NOT": lambda self: self._parse_not_constraint(),
        "NULL": lambda self: self.expression(exp.NotNullColumnConstraint, allow_null=True),
        "ON": lambda self: self._match(TokenType.UPDATE)
        and self.expression(exp.OnUpdateColumnConstraint, this=self._parse_function()),
        "PATH": lambda self: self.expression(exp.PathColumnConstraint, this=self._parse_string()),
        "PRIMARY KEY": lambda self: self._parse_primary_key(),
        "REFERENCES": lambda self: self._parse_references(match=False),
        "TITLE": lambda self: self.expression(
            exp.TitleColumnConstraint, this=self._parse_var_or_string()
        ),
        "TTL": lambda self: self.expression(exp.MergeTreeTTL, expressions=[self._parse_bitwise()]),
        "UNIQUE": lambda self: self._parse_unique(),
        "UPPERCASE": lambda self: self.expression(exp.UppercaseColumnConstraint),
    }

    ALTER_PARSERS = {
        "ADD": lambda self: self._parse_alter_table_add(),
        "ALTER": lambda self: self._parse_alter_table_alter(),
        "DELETE": lambda self: self.expression(exp.Delete, where=self._parse_where()),
        "DROP": lambda self: self._parse_alter_table_drop(),
        "RENAME": lambda self: self._parse_alter_table_rename(),
    }

    SCHEMA_UNNAMED_CONSTRAINTS = {"CHECK", "FOREIGN KEY", "LIKE", "PRIMARY KEY", "UNIQUE"}

    NO_PAREN_FUNCTION_PARSERS = {
        TokenType.ANY: lambda self: self.expression(exp.Any, this=self._parse_bitwise()),
        TokenType.CASE: lambda self: self._parse_case(),
        TokenType.IF: lambda self: self._parse_if(),
        TokenType.NEXT_VALUE_FOR: lambda self: self.expression(
            exp.NextValueFor,
            this=self._parse_column(),
            order=self._match(TokenType.OVER) and self._parse_wrapped(self._parse_order),
        ),
    }

    FUNCTIONS_WITH_ALIASED_ARGS = {"STRUCT"}

    FUNCTION_PARSERS = {
        "ANY_VALUE": lambda self: self._parse_any_value(),
        "CAST": lambda self: self._parse_cast(self.STRICT_CAST),
        "CONCAT": lambda self: self._parse_concat(),
        "CONVERT": lambda self: self._parse_convert(self.STRICT_CAST),
        "DECODE": lambda self: self._parse_decode(),
        "EXTRACT": lambda self: self._parse_extract(),
        "JSON_OBJECT": lambda self: self._parse_json_object(),
        "LOG": lambda self: self._parse_logarithm(),
        "MATCH": lambda self: self._parse_match_against(),
        "OPENJSON": lambda self: self._parse_open_json(),
        "POSITION": lambda self: self._parse_position(),
        "SAFE_CAST": lambda self: self._parse_cast(False),
        "STRING_AGG": lambda self: self._parse_string_agg(),
        "SUBSTRING": lambda self: self._parse_substring(),
        "TRIM": lambda self: self._parse_trim(),
        "TRY_CAST": lambda self: self._parse_cast(False),
        "TRY_CONVERT": lambda self: self._parse_convert(False),
    }

    QUERY_MODIFIER_PARSERS = {
        TokenType.MATCH_RECOGNIZE: lambda self: ("match", self._parse_match_recognize()),
        TokenType.WHERE: lambda self: ("where", self._parse_where()),
        TokenType.GROUP_BY: lambda self: ("group", self._parse_group()),
        TokenType.HAVING: lambda self: ("having", self._parse_having()),
        TokenType.QUALIFY: lambda self: ("qualify", self._parse_qualify()),
        TokenType.WINDOW: lambda self: ("windows", self._parse_window_clause()),
        TokenType.ORDER_BY: lambda self: ("order", self._parse_order()),
        TokenType.LIMIT: lambda self: ("limit", self._parse_limit()),
        TokenType.FETCH: lambda self: ("limit", self._parse_limit()),
        TokenType.OFFSET: lambda self: ("offset", self._parse_offset()),
        TokenType.FOR: lambda self: ("locks", self._parse_locks()),
        TokenType.LOCK: lambda self: ("locks", self._parse_locks()),
        TokenType.TABLE_SAMPLE: lambda self: ("sample", self._parse_table_sample(as_modifier=True)),
        TokenType.USING: lambda self: ("sample", self._parse_table_sample(as_modifier=True)),
        TokenType.CLUSTER_BY: lambda self: (
            "cluster",
            self._parse_sort(exp.Cluster, TokenType.CLUSTER_BY),
        ),
        TokenType.DISTRIBUTE_BY: lambda self: (
            "distribute",
            self._parse_sort(exp.Distribute, TokenType.DISTRIBUTE_BY),
        ),
        TokenType.SORT_BY: lambda self: ("sort", self._parse_sort(exp.Sort, TokenType.SORT_BY)),
    }

    SET_PARSERS = {
        "GLOBAL": lambda self: self._parse_set_item_assignment("GLOBAL"),
        "LOCAL": lambda self: self._parse_set_item_assignment("LOCAL"),
        "SESSION": lambda self: self._parse_set_item_assignment("SESSION"),
        "TRANSACTION": lambda self: self._parse_set_transaction(),
    }

    SHOW_PARSERS: t.Dict[str, t.Callable] = {}

    TYPE_LITERAL_PARSERS: t.Dict[exp.DataType.Type, t.Callable] = {}

    MODIFIABLES = (exp.Subquery, exp.Subqueryable, exp.Table)

    DDL_SELECT_TOKENS = {TokenType.SELECT, TokenType.WITH, TokenType.L_PAREN}

    PRE_VOLATILE_TOKENS = {TokenType.CREATE, TokenType.REPLACE, TokenType.UNIQUE}

    TRANSACTION_KIND = {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}
    TRANSACTION_CHARACTERISTICS = {
        "ISOLATION LEVEL REPEATABLE READ",
        "ISOLATION LEVEL READ COMMITTED",
        "ISOLATION LEVEL READ UNCOMMITTED",
        "ISOLATION LEVEL SERIALIZABLE",
        "READ WRITE",
        "READ ONLY",
    }

    INSERT_ALTERNATIVES = {"ABORT", "FAIL", "IGNORE", "REPLACE", "ROLLBACK"}

    CLONE_KINDS = {"TIMESTAMP", "OFFSET", "STATEMENT"}

    TABLE_INDEX_HINT_TOKENS = {TokenType.FORCE, TokenType.IGNORE, TokenType.USE}

    WINDOW_ALIAS_TOKENS = ID_VAR_TOKENS - {TokenType.ROWS}
    WINDOW_BEFORE_PAREN_TOKENS = {TokenType.OVER}
    WINDOW_SIDES = {"FOLLOWING", "PRECEDING"}

    ADD_CONSTRAINT_TOKENS = {TokenType.CONSTRAINT, TokenType.PRIMARY_KEY, TokenType.FOREIGN_KEY}

    STRICT_CAST = True

    # A NULL arg in CONCAT yields NULL by default
    CONCAT_NULL_OUTPUTS_STRING = False

    PREFIXED_PIVOT_COLUMNS = False
    IDENTIFY_PIVOT_STRINGS = False

    LOG_BASE_FIRST = True
    LOG_DEFAULTS_TO_LN = False

    __slots__ = (
        "error_level",
        "error_message_context",
        "max_errors",
        "sql",
        "errors",
        "_tokens",
        "_index",
        "_curr",
        "_next",
        "_prev",
        "_prev_comments",
    )

    # Autofilled
    INDEX_OFFSET: int = 0
    UNNEST_COLUMN_ONLY: bool = False
    ALIAS_POST_TABLESAMPLE: bool = False
    STRICT_STRING_CONCAT = False
    NULL_ORDERING: str = "nulls_are_small"
    SHOW_TRIE: t.Dict = {}
    SET_TRIE: t.Dict = {}
    FORMAT_MAPPING: t.Dict[str, str] = {}
    FORMAT_TRIE: t.Dict = {}
    TIME_MAPPING: t.Dict[str, str] = {}
    TIME_TRIE: t.Dict = {}

    def __init__(
        self,
        error_level: t.Optional[ErrorLevel] = None,
        error_message_context: int = 100,
        max_errors: int = 3,
    ):
        self.error_level = error_level or ErrorLevel.IMMEDIATE
        self.error_message_context = error_message_context
        self.max_errors = max_errors
        self.reset()

    def reset(self):
        self.sql = ""
        self.errors = []
        self._tokens = []
        self._index = 0
        self._curr = None
        self._next = None
        self._prev = None
        self._prev_comments = None

    def parse(
        self, raw_tokens: t.List[Token], sql: t.Optional[str] = None
    ) -> t.List[t.Optional[exp.Expression]]:
        """
        Parses a list of tokens and returns a list of syntax trees, one tree
        per parsed SQL statement.

        Args:
            raw_tokens: The list of tokens.
            sql: The original SQL string, used to produce helpful debug messages.

        Returns:
            The list of the produced syntax trees.
        """
        return self._parse(
            parse_method=self.__class__._parse_statement, raw_tokens=raw_tokens, sql=sql
        )

    def parse_into(
        self,
        expression_types: exp.IntoType,
        raw_tokens: t.List[Token],
        sql: t.Optional[str] = None,
    ) -> t.List[t.Optional[exp.Expression]]:
        """
        Parses a list of tokens into a given Expression type. If a collection of Expression
        types is given instead, this method will try to parse the token list into each one
        of them, stopping at the first for which the parsing succeeds.

        Args:
            expression_types: The expression type(s) to try and parse the token list into.
            raw_tokens: The list of tokens.
            sql: The original SQL string, used to produce helpful debug messages.

        Returns:
            The target Expression.
        """
        errors = []
        for expression_type in ensure_list(expression_types):
            parser = self.EXPRESSION_PARSERS.get(expression_type)
            if not parser:
                raise TypeError(f"No parser registered for {expression_type}")

            try:
                return self._parse(parser, raw_tokens, sql)
            except ParseError as e:
                e.errors[0]["into_expression"] = expression_type
                errors.append(e)

        raise ParseError(
            f"Failed to parse '{sql or raw_tokens}' into {expression_types}",
            errors=merge_errors(errors),
        ) from errors[-1]

    def _parse(
        self,
        parse_method: t.Callable[[Parser], t.Optional[exp.Expression]],
        raw_tokens: t.List[Token],
        sql: t.Optional[str] = None,
    ) -> t.List[t.Optional[exp.Expression]]:
        self.reset()
        self.sql = sql or ""

        total = len(raw_tokens)
        chunks: t.List[t.List[Token]] = [[]]

        for i, token in enumerate(raw_tokens):
            if token.token_type == TokenType.SEMICOLON:
                if i < total - 1:
                    chunks.append([])
            else:
                chunks[-1].append(token)

        expressions = []

        for tokens in chunks:
            self._index = -1
            self._tokens = tokens
            self._advance()

            expressions.append(parse_method(self))

            if self._index < len(self._tokens):
                self.raise_error("Invalid expression / Unexpected token")

            self.check_errors()

        return expressions

    def check_errors(self) -> None:
        """Logs or raises any found errors, depending on the chosen error level setting."""
        if self.error_level == ErrorLevel.WARN:
            for error in self.errors:
                logger.error(str(error))
        elif self.error_level == ErrorLevel.RAISE and self.errors:
            raise ParseError(
                concat_messages(self.errors, self.max_errors),
                errors=merge_errors(self.errors),
            )

    def raise_error(self, message: str, token: t.Optional[Token] = None) -> None:
        """
        Appends an error in the list of recorded errors or raises it, depending on the chosen
        error level setting.
        """
        token = token or self._curr or self._prev or Token.string("")
        start = token.start
        end = token.end + 1
        start_context = self.sql[max(start - self.error_message_context, 0) : start]
        highlight = self.sql[start:end]
        end_context = self.sql[end : end + self.error_message_context]

        error = ParseError.new(
            f"{message}. Line {token.line}, Col: {token.col}.\n"
            f"  {start_context}\033[4m{highlight}\033[0m{end_context}",
            description=message,
            line=token.line,
            col=token.col,
            start_context=start_context,
            highlight=highlight,
            end_context=end_context,
        )

        if self.error_level == ErrorLevel.IMMEDIATE:
            raise error

        self.errors.append(error)

    def expression(
        self, exp_class: t.Type[E], comments: t.Optional[t.List[str]] = None, **kwargs
    ) -> E:
        """
        Creates a new, validated Expression.

        Args:
            exp_class: The expression class to instantiate.
            comments: An optional list of comments to attach to the expression.
            kwargs: The arguments to set for the expression along with their respective values.

        Returns:
            The target expression.
        """
        instance = exp_class(**kwargs)
        instance.add_comments(comments) if comments else self._add_comments(instance)
        return self.validate_expression(instance)

    def _add_comments(self, expression: t.Optional[exp.Expression]) -> None:
        if expression and self._prev_comments:
            expression.add_comments(self._prev_comments)
            self._prev_comments = None

    def validate_expression(self, expression: E, args: t.Optional[t.List] = None) -> E:
        """
        Validates an Expression, making sure that all its mandatory arguments are set.

        Args:
            expression: The expression to validate.
            args: An optional list of items that was used to instantiate the expression, if it's a Func.

        Returns:
            The validated expression.
        """
        if self.error_level != ErrorLevel.IGNORE:
            for error_message in expression.error_messages(args):
                self.raise_error(error_message)

        return expression

    def _find_sql(self, start: Token, end: Token) -> str:
        return self.sql[start.start : end.end + 1]

    def _advance(self, times: int = 1) -> None:
        self._index += times
        self._curr = seq_get(self._tokens, self._index)
        self._next = seq_get(self._tokens, self._index + 1)

        if self._index > 0:
            self._prev = self._tokens[self._index - 1]
            self._prev_comments = self._prev.comments
        else:
            self._prev = None
            self._prev_comments = None

    def _retreat(self, index: int) -> None:
        if index != self._index:
            self._advance(index - self._index)

    def _parse_command(self) -> exp.Command:
        return self.expression(exp.Command, this=self._prev.text, expression=self._parse_string())

    def _parse_comment(self, allow_exists: bool = True) -> exp.Expression:
        start = self._prev
        exists = self._parse_exists() if allow_exists else None

        self._match(TokenType.ON)

        kind = self._match_set(self.CREATABLES) and self._prev
        if not kind:
            return self._parse_as_command(start)

        if kind.token_type in (TokenType.FUNCTION, TokenType.PROCEDURE):
            this = self._parse_user_defined_function(kind=kind.token_type)
        elif kind.token_type == TokenType.TABLE:
            this = self._parse_table(alias_tokens=self.COMMENT_TABLE_ALIAS_TOKENS)
        elif kind.token_type == TokenType.COLUMN:
            this = self._parse_column()
        else:
            this = self._parse_id_var()

        self._match(TokenType.IS)

        return self.expression(
            exp.Comment, this=this, kind=kind.text, expression=self._parse_string(), exists=exists
        )

    def _parse_to_table(
        self,
    ) -> exp.ToTableProperty:
        table = self._parse_table_parts(schema=True)
        return self.expression(exp.ToTableProperty, this=table)

    # https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#mergetree-table-ttl
    def _parse_ttl(self) -> exp.Expression:
        def _parse_ttl_action() -> t.Optional[exp.Expression]:
            this = self._parse_bitwise()

            if self._match_text_seq("DELETE"):
                return self.expression(exp.MergeTreeTTLAction, this=this, delete=True)
            if self._match_text_seq("RECOMPRESS"):
                return self.expression(
                    exp.MergeTreeTTLAction, this=this, recompress=self._parse_bitwise()
                )
            if self._match_text_seq("TO", "DISK"):
                return self.expression(
                    exp.MergeTreeTTLAction, this=this, to_disk=self._parse_string()
                )
            if self._match_text_seq("TO", "VOLUME"):
                return self.expression(
                    exp.MergeTreeTTLAction, this=this, to_volume=self._parse_string()
                )

            return this

        expressions = self._parse_csv(_parse_ttl_action)
        where = self._parse_where()
        group = self._parse_group()

        aggregates = None
        if group and self._match(TokenType.SET):
            aggregates = self._parse_csv(self._parse_set_item)

        return self.expression(
            exp.MergeTreeTTL,
            expressions=expressions,
            where=where,
            group=group,
            aggregates=aggregates,
        )

    def _parse_statement(self) -> t.Optional[exp.Expression]:
        if self._curr is None:
            return None

        if self._match_set(self.STATEMENT_PARSERS):
            return self.STATEMENT_PARSERS[self._prev.token_type](self)

        if self._match_set(Tokenizer.COMMANDS):
            return self._parse_command()

        expression = self._parse_expression()
        expression = self._parse_set_operations(expression) if expression else self._parse_select()
        return self._parse_query_modifiers(expression)

    def _parse_drop(self) -> exp.Drop | exp.Command:
        start = self._prev
        temporary = self._match(TokenType.TEMPORARY)
        materialized = self._match_text_seq("MATERIALIZED")

        kind = self._match_set(self.CREATABLES) and self._prev.text
        if not kind:
            return self._parse_as_command(start)

        return self.expression(
            exp.Drop,
            comments=start.comments,
            exists=self._parse_exists(),
            this=self._parse_table(schema=True),
            kind=kind,
            temporary=temporary,
            materialized=materialized,
            cascade=self._match_text_seq("CASCADE"),
            constraints=self._match_text_seq("CONSTRAINTS"),
            purge=self._match_text_seq("PURGE"),
        )

    def _parse_exists(self, not_: bool = False) -> t.Optional[bool]:
        return (
            self._match(TokenType.IF)
            and (not not_ or self._match(TokenType.NOT))
            and self._match(TokenType.EXISTS)
        )

    def _parse_create(self) -> exp.Create | exp.Command:
        # Note: this can't be None because we've matched a statement parser
        start = self._prev
        replace = start.text.upper() == "REPLACE" or self._match_pair(
            TokenType.OR, TokenType.REPLACE
        )
        unique = self._match(TokenType.UNIQUE)

        if self._match_pair(TokenType.TABLE, TokenType.FUNCTION, advance=False):
            self._advance()

        properties = None
        create_token = self._match_set(self.CREATABLES) and self._prev

        if not create_token:
            # exp.Properties.Location.POST_CREATE
            properties = self._parse_properties()
            create_token = self._match_set(self.CREATABLES) and self._prev

            if not properties or not create_token:
                return self._parse_as_command(start)

        exists = self._parse_exists(not_=True)
        this = None
        expression = None
        indexes = None
        no_schema_binding = None
        begin = None
        clone = None

        def extend_props(temp_props: t.Optional[exp.Properties]) -> None:
            nonlocal properties
            if properties and temp_props:
                properties.expressions.extend(temp_props.expressions)
            elif temp_props:
                properties = temp_props

        if create_token.token_type in (TokenType.FUNCTION, TokenType.PROCEDURE):
            this = self._parse_user_defined_function(kind=create_token.token_type)

            # exp.Properties.Location.POST_SCHEMA ("schema" here is the UDF's type signature)
            extend_props(self._parse_properties())

            self._match(TokenType.ALIAS)
            begin = self._match(TokenType.BEGIN)
            return_ = self._match_text_seq("RETURN")
            expression = self._parse_statement()

            if return_:
                expression = self.expression(exp.Return, this=expression)
        elif create_token.token_type == TokenType.INDEX:
            this = self._parse_index(index=self._parse_id_var())
        elif create_token.token_type in self.DB_CREATABLES:
            table_parts = self._parse_table_parts(schema=True)

            # exp.Properties.Location.POST_NAME
            self._match(TokenType.COMMA)
            extend_props(self._parse_properties(before=True))

            this = self._parse_schema(this=table_parts)

            # exp.Properties.Location.POST_SCHEMA and POST_WITH
            extend_props(self._parse_properties())

            self._match(TokenType.ALIAS)
            if not self._match_set(self.DDL_SELECT_TOKENS, advance=False):
                # exp.Properties.Location.POST_ALIAS
                extend_props(self._parse_properties())

            expression = self._parse_ddl_select()

            if create_token.token_type == TokenType.TABLE:
                # exp.Properties.Location.POST_EXPRESSION
                extend_props(self._parse_properties())

                indexes = []
                while True:
                    index = self._parse_index()

                    # exp.Properties.Location.POST_INDEX
                    extend_props(self._parse_properties())

                    if not index:
                        break
                    else:
                        self._match(TokenType.COMMA)
                        indexes.append(index)
            elif create_token.token_type == TokenType.VIEW:
                if self._match_text_seq("WITH", "NO", "SCHEMA", "BINDING"):
                    no_schema_binding = True

            if self._match_text_seq("CLONE"):
                clone = self._parse_table(schema=True)
                when = self._match_texts({"AT", "BEFORE"}) and self._prev.text.upper()
                clone_kind = (
                    self._match(TokenType.L_PAREN)
                    and self._match_texts(self.CLONE_KINDS)
                    and self._prev.text.upper()
                )
                clone_expression = self._match(TokenType.FARROW) and self._parse_bitwise()
                self._match(TokenType.R_PAREN)
                clone = self.expression(
                    exp.Clone, this=clone, when=when, kind=clone_kind, expression=clone_expression
                )

        return self.expression(
            exp.Create,
            this=this,
            kind=create_token.text,
            replace=replace,
            unique=unique,
            expression=expression,
            exists=exists,
            properties=properties,
            indexes=indexes,
            no_schema_binding=no_schema_binding,
            begin=begin,
            clone=clone,
        )

    def _parse_property_before(self) -> t.Optional[exp.Expression]:
        # only used for teradata currently
        self._match(TokenType.COMMA)

        kwargs = {
            "no": self._match_text_seq("NO"),
            "dual": self._match_text_seq("DUAL"),
            "before": self._match_text_seq("BEFORE"),
            "default": self._match_text_seq("DEFAULT"),
            "local": (self._match_text_seq("LOCAL") and "LOCAL")
            or (self._match_text_seq("NOT", "LOCAL") and "NOT LOCAL"),
            "after": self._match_text_seq("AFTER"),
            "minimum": self._match_texts(("MIN", "MINIMUM")),
            "maximum": self._match_texts(("MAX", "MAXIMUM")),
        }

        if self._match_texts(self.PROPERTY_PARSERS):
            parser = self.PROPERTY_PARSERS[self._prev.text.upper()]
            try:
                return parser(self, **{k: v for k, v in kwargs.items() if v})
            except TypeError:
                self.raise_error(f"Cannot parse property '{self._prev.text}'")

        return None

    def _parse_property(self) -> t.Optional[exp.Expression]:
        if self._match_texts(self.PROPERTY_PARSERS):
            return self.PROPERTY_PARSERS[self._prev.text.upper()](self)

        if self._match_pair(TokenType.DEFAULT, TokenType.CHARACTER_SET):
            return self._parse_character_set(default=True)

        if self._match_text_seq("COMPOUND", "SORTKEY"):
            return self._parse_sortkey(compound=True)

        if self._match_text_seq("SQL", "SECURITY"):
            return self.expression(exp.SqlSecurityProperty, definer=self._match_text_seq("DEFINER"))

        assignment = self._match_pair(
            TokenType.VAR, TokenType.EQ, advance=False
        ) or self._match_pair(TokenType.STRING, TokenType.EQ, advance=False)

        if assignment:
            key = self._parse_var_or_string()
            self._match(TokenType.EQ)
            return self.expression(exp.Property, this=key, value=self._parse_column())

        return None

    def _parse_stored(self) -> exp.FileFormatProperty:
        self._match(TokenType.ALIAS)

        input_format = self._parse_string() if self._match_text_seq("INPUTFORMAT") else None
        output_format = self._parse_string() if self._match_text_seq("OUTPUTFORMAT") else None

        return self.expression(
            exp.FileFormatProperty,
            this=self.expression(
                exp.InputOutputFormat, input_format=input_format, output_format=output_format
            )
            if input_format or output_format
            else self._parse_var_or_string() or self._parse_number() or self._parse_id_var(),
        )

    def _parse_property_assignment(self, exp_class: t.Type[E]) -> E:
        self._match(TokenType.EQ)
        self._match(TokenType.ALIAS)
        return self.expression(exp_class, this=self._parse_field())

    def _parse_properties(self, before: t.Optional[bool] = None) -> t.Optional[exp.Properties]:
        properties = []
        while True:
            if before:
                prop = self._parse_property_before()
            else:
                prop = self._parse_property()

            if not prop:
                break
            for p in ensure_list(prop):
                properties.append(p)

        if properties:
            return self.expression(exp.Properties, expressions=properties)

        return None

    def _parse_fallback(self, no: bool = False) -> exp.FallbackProperty:
        return self.expression(
            exp.FallbackProperty, no=no, protection=self._match_text_seq("PROTECTION")
        )

    def _parse_volatile_property(self) -> exp.VolatileProperty | exp.StabilityProperty:
        if self._index >= 2:
            pre_volatile_token = self._tokens[self._index - 2]
        else:
            pre_volatile_token = None

        if pre_volatile_token and pre_volatile_token.token_type in self.PRE_VOLATILE_TOKENS:
            return exp.VolatileProperty()

        return self.expression(exp.StabilityProperty, this=exp.Literal.string("VOLATILE"))

    def _parse_with_property(
        self,
    ) -> t.Optional[exp.Expression] | t.List[t.Optional[exp.Expression]]:
        if self._match(TokenType.L_PAREN, advance=False):
            return self._parse_wrapped_csv(self._parse_property)

        if self._match_text_seq("JOURNAL"):
            return self._parse_withjournaltable()

        if self._match_text_seq("DATA"):
            return self._parse_withdata(no=False)
        elif self._match_text_seq("NO", "DATA"):
            return self._parse_withdata(no=True)

        if not self._next:
            return None

        return self._parse_withisolatedloading()

    # https://dev.mysql.com/doc/refman/8.0/en/create-view.html
    def _parse_definer(self) -> t.Optional[exp.DefinerProperty]:
        self._match(TokenType.EQ)

        user = self._parse_id_var()
        self._match(TokenType.PARAMETER)
        host = self._parse_id_var() or (self._match(TokenType.MOD) and self._prev.text)

        if not user or not host:
            return None

        return exp.DefinerProperty(this=f"{user}@{host}")

    def _parse_withjournaltable(self) -> exp.WithJournalTableProperty:
        self._match(TokenType.TABLE)
        self._match(TokenType.EQ)
        return self.expression(exp.WithJournalTableProperty, this=self._parse_table_parts())

    def _parse_log(self, no: bool = False) -> exp.LogProperty:
        return self.expression(exp.LogProperty, no=no)

    def _parse_journal(self, **kwargs) -> exp.JournalProperty:
        return self.expression(exp.JournalProperty, **kwargs)

    def _parse_checksum(self) -> exp.ChecksumProperty:
        self._match(TokenType.EQ)

        on = None
        if self._match(TokenType.ON):
            on = True
        elif self._match_text_seq("OFF"):
            on = False

        return self.expression(exp.ChecksumProperty, on=on, default=self._match(TokenType.DEFAULT))

    def _parse_cluster(self) -> exp.Cluster:
        return self.expression(exp.Cluster, expressions=self._parse_csv(self._parse_ordered))

    def _parse_clustered_by(self) -> exp.ClusteredByProperty:
        self._match_text_seq("BY")

        self._match_l_paren()
        expressions = self._parse_csv(self._parse_column)
        self._match_r_paren()

        if self._match_text_seq("SORTED", "BY"):
            self._match_l_paren()
            sorted_by = self._parse_csv(self._parse_ordered)
            self._match_r_paren()
        else:
            sorted_by = None

        self._match(TokenType.INTO)
        buckets = self._parse_number()
        self._match_text_seq("BUCKETS")

        return self.expression(
            exp.ClusteredByProperty,
            expressions=expressions,
            sorted_by=sorted_by,
            buckets=buckets,
        )

    def _parse_copy_property(self) -> t.Optional[exp.CopyGrantsProperty]:
        if not self._match_text_seq("GRANTS"):
            self._retreat(self._index - 1)
            return None

        return self.expression(exp.CopyGrantsProperty)

    def _parse_freespace(self) -> exp.FreespaceProperty:
        self._match(TokenType.EQ)
        return self.expression(
            exp.FreespaceProperty, this=self._parse_number(), percent=self._match(TokenType.PERCENT)
        )

    def _parse_mergeblockratio(
        self, no: bool = False, default: bool = False
    ) -> exp.MergeBlockRatioProperty:
        if self._match(TokenType.EQ):
            return self.expression(
                exp.MergeBlockRatioProperty,
                this=self._parse_number(),
                percent=self._match(TokenType.PERCENT),
            )

        return self.expression(exp.MergeBlockRatioProperty, no=no, default=default)

    def _parse_datablocksize(
        self,
        default: t.Optional[bool] = None,
        minimum: t.Optional[bool] = None,
        maximum: t.Optional[bool] = None,
    ) -> exp.DataBlocksizeProperty:
        self._match(TokenType.EQ)
        size = self._parse_number()

        units = None
        if self._match_texts(("BYTES", "KBYTES", "KILOBYTES")):
            units = self._prev.text

        return self.expression(
            exp.DataBlocksizeProperty,
            size=size,
            units=units,
            default=default,
            minimum=minimum,
            maximum=maximum,
        )

    def _parse_blockcompression(self) -> exp.BlockCompressionProperty:
        self._match(TokenType.EQ)
        always = self._match_text_seq("ALWAYS")
        manual = self._match_text_seq("MANUAL")
        never = self._match_text_seq("NEVER")
        default = self._match_text_seq("DEFAULT")

        autotemp = None
        if self._match_text_seq("AUTOTEMP"):
            autotemp = self._parse_schema()

        return self.expression(
            exp.BlockCompressionProperty,
            always=always,
            manual=manual,
            never=never,
            default=default,
            autotemp=autotemp,
        )

    def _parse_withisolatedloading(self) -> exp.IsolatedLoadingProperty:
        no = self._match_text_seq("NO")
        concurrent = self._match_text_seq("CONCURRENT")
        self._match_text_seq("ISOLATED", "LOADING")
        for_all = self._match_text_seq("FOR", "ALL")
        for_insert = self._match_text_seq("FOR", "INSERT")
        for_none = self._match_text_seq("FOR", "NONE")
        return self.expression(
            exp.IsolatedLoadingProperty,
            no=no,
            concurrent=concurrent,
            for_all=for_all,
            for_insert=for_insert,
            for_none=for_none,
        )

    def _parse_locking(self) -> exp.LockingProperty:
        if self._match(TokenType.TABLE):
            kind = "TABLE"
        elif self._match(TokenType.VIEW):
            kind = "VIEW"
        elif self._match(TokenType.ROW):
            kind = "ROW"
        elif self._match_text_seq("DATABASE"):
            kind = "DATABASE"
        else:
            kind = None

        if kind in ("DATABASE", "TABLE", "VIEW"):
            this = self._parse_table_parts()
        else:
            this = None

        if self._match(TokenType.FOR):
            for_or_in = "FOR"
        elif self._match(TokenType.IN):
            for_or_in = "IN"
        else:
            for_or_in = None

        if self._match_text_seq("ACCESS"):
            lock_type = "ACCESS"
        elif self._match_texts(("EXCL", "EXCLUSIVE")):
            lock_type = "EXCLUSIVE"
        elif self._match_text_seq("SHARE"):
            lock_type = "SHARE"
        elif self._match_text_seq("READ"):
            lock_type = "READ"
        elif self._match_text_seq("WRITE"):
            lock_type = "WRITE"
        elif self._match_text_seq("CHECKSUM"):
            lock_type = "CHECKSUM"
        else:
            lock_type = None

        override = self._match_text_seq("OVERRIDE")

        return self.expression(
            exp.LockingProperty,
            this=this,
            kind=kind,
            for_or_in=for_or_in,
            lock_type=lock_type,
            override=override,
        )

    def _parse_partition_by(self) -> t.List[t.Optional[exp.Expression]]:
        if self._match(TokenType.PARTITION_BY):
            return self._parse_csv(self._parse_conjunction)
        return []

    def _parse_partitioned_by(self) -> exp.PartitionedByProperty:
        self._match(TokenType.EQ)
        return self.expression(
            exp.PartitionedByProperty,
            this=self._parse_schema() or self._parse_bracket(self._parse_field()),
        )

    def _parse_withdata(self, no: bool = False) -> exp.WithDataProperty:
        if self._match_text_seq("AND", "STATISTICS"):
            statistics = True
        elif self._match_text_seq("AND", "NO", "STATISTICS"):
            statistics = False
        else:
            statistics = None

        return self.expression(exp.WithDataProperty, no=no, statistics=statistics)

    def _parse_no_property(self) -> t.Optional[exp.NoPrimaryIndexProperty]:
        if self._match_text_seq("PRIMARY", "INDEX"):
            return exp.NoPrimaryIndexProperty()
        return None

    def _parse_on_property(self) -> t.Optional[exp.Expression]:
        if self._match_text_seq("COMMIT", "PRESERVE", "ROWS"):
            return exp.OnCommitProperty()
        elif self._match_text_seq("COMMIT", "DELETE", "ROWS"):
            return exp.OnCommitProperty(delete=True)
        return None

    def _parse_distkey(self) -> exp.DistKeyProperty:
        return self.expression(exp.DistKeyProperty, this=self._parse_wrapped(self._parse_id_var))

    def _parse_create_like(self) -> t.Optional[exp.LikeProperty]:
        table = self._parse_table(schema=True)

        options = []
        while self._match_texts(("INCLUDING", "EXCLUDING")):
            this = self._prev.text.upper()

            id_var = self._parse_id_var()
            if not id_var:
                return None

            options.append(
                self.expression(exp.Property, this=this, value=exp.var(id_var.this.upper()))
            )

        return self.expression(exp.LikeProperty, this=table, expressions=options)

    def _parse_sortkey(self, compound: bool = False) -> exp.SortKeyProperty:
        return self.expression(
            exp.SortKeyProperty, this=self._parse_wrapped_id_vars(), compound=compound
        )

    def _parse_character_set(self, default: bool = False) -> exp.CharacterSetProperty:
        self._match(TokenType.EQ)
        return self.expression(
            exp.CharacterSetProperty, this=self._parse_var_or_string(), default=default
        )

    def _parse_returns(self) -> exp.ReturnsProperty:
        value: t.Optional[exp.Expression]
        is_table = self._match(TokenType.TABLE)

        if is_table:
            if self._match(TokenType.LT):
                value = self.expression(
                    exp.Schema,
                    this="TABLE",
                    expressions=self._parse_csv(self._parse_struct_types),
                )
                if not self._match(TokenType.GT):
                    self.raise_error("Expecting >")
            else:
                value = self._parse_schema(exp.var("TABLE"))
        else:
            value = self._parse_types()

        return self.expression(exp.ReturnsProperty, this=value, is_table=is_table)

    def _parse_describe(self) -> exp.Describe:
        kind = self._match_set(self.CREATABLES) and self._prev.text
        this = self._parse_table()
        return self.expression(exp.Describe, this=this, kind=kind)

    def _parse_insert(self) -> exp.Insert:
        overwrite = self._match(TokenType.OVERWRITE)
        ignore = self._match(TokenType.IGNORE)
        local = self._match_text_seq("LOCAL")
        alternative = None

        if self._match_text_seq("DIRECTORY"):
            this: t.Optional[exp.Expression] = self.expression(
                exp.Directory,
                this=self._parse_var_or_string(),
                local=local,
                row_format=self._parse_row_format(match_row=True),
            )
        else:
            if self._match(TokenType.OR):
                alternative = self._match_texts(self.INSERT_ALTERNATIVES) and self._prev.text

            self._match(TokenType.INTO)
            self._match(TokenType.TABLE)
            this = self._parse_table(schema=True)

        returning = self._parse_returning()

        return self.expression(
            exp.Insert,
            this=this,
            exists=self._parse_exists(),
            partition=self._parse_partition(),
            where=self._match_pair(TokenType.REPLACE, TokenType.WHERE)
            and self._parse_conjunction(),
            expression=self._parse_ddl_select(),
            conflict=self._parse_on_conflict(),
            returning=returning or self._parse_returning(),
            overwrite=overwrite,
            alternative=alternative,
            ignore=ignore,
        )

    def _parse_on_conflict(self) -> t.Optional[exp.OnConflict]:
        conflict = self._match_text_seq("ON", "CONFLICT")
        duplicate = self._match_text_seq("ON", "DUPLICATE", "KEY")

        if not conflict and not duplicate:
            return None

        nothing = None
        expressions = None
        key = None
        constraint = None

        if conflict:
            if self._match_text_seq("ON", "CONSTRAINT"):
                constraint = self._parse_id_var()
            else:
                key = self._parse_csv(self._parse_value)

        self._match_text_seq("DO")
        if self._match_text_seq("NOTHING"):
            nothing = True
        else:
            self._match(TokenType.UPDATE)
            self._match(TokenType.SET)
            expressions = self._parse_csv(self._parse_equality)

        return self.expression(
            exp.OnConflict,
            duplicate=duplicate,
            expressions=expressions,
            nothing=nothing,
            key=key,
            constraint=constraint,
        )

    def _parse_returning(self) -> t.Optional[exp.Returning]:
        if not self._match(TokenType.RETURNING):
            return None
        return self.expression(
            exp.Returning,
            expressions=self._parse_csv(self._parse_expression),
            into=self._match(TokenType.INTO) and self._parse_table_part(),
        )

    def _parse_row(self) -> t.Optional[exp.RowFormatSerdeProperty | exp.RowFormatDelimitedProperty]:
        if not self._match(TokenType.FORMAT):
            return None
        return self._parse_row_format()

    def _parse_row_format(
        self, match_row: bool = False
    ) -> t.Optional[exp.RowFormatSerdeProperty | exp.RowFormatDelimitedProperty]:
        if match_row and not self._match_pair(TokenType.ROW, TokenType.FORMAT):
            return None

        if self._match_text_seq("SERDE"):
            this = self._parse_string()

            serde_properties = None
            if self._match(TokenType.SERDE_PROPERTIES):
                serde_properties = self.expression(
                    exp.SerdeProperties, expressions=self._parse_wrapped_csv(self._parse_property)
                )

            return self.expression(
                exp.RowFormatSerdeProperty, this=this, serde_properties=serde_properties
            )

        self._match_text_seq("DELIMITED")

        kwargs = {}

        if self._match_text_seq("FIELDS", "TERMINATED", "BY"):
            kwargs["fields"] = self._parse_string()
            if self._match_text_seq("ESCAPED", "BY"):
                kwargs["escaped"] = self._parse_string()
        if self._match_text_seq("COLLECTION", "ITEMS", "TERMINATED", "BY"):
            kwargs["collection_items"] = self._parse_string()
        if self._match_text_seq("MAP", "KEYS", "TERMINATED", "BY"):
            kwargs["map_keys"] = self._parse_string()
        if self._match_text_seq("LINES", "TERMINATED", "BY"):
            kwargs["lines"] = self._parse_string()
        if self._match_text_seq("NULL", "DEFINED", "AS"):
            kwargs["null"] = self._parse_string()

        return self.expression(exp.RowFormatDelimitedProperty, **kwargs)  # type: ignore

    def _parse_load(self) -> exp.LoadData | exp.Command:
        if self._match_text_seq("DATA"):
            local = self._match_text_seq("LOCAL")
            self._match_text_seq("INPATH")
            inpath = self._parse_string()
            overwrite = self._match(TokenType.OVERWRITE)
            self._match_pair(TokenType.INTO, TokenType.TABLE)

            return self.expression(
                exp.LoadData,
                this=self._parse_table(schema=True),
                local=local,
                overwrite=overwrite,
                inpath=inpath,
                partition=self._parse_partition(),
                input_format=self._match_text_seq("INPUTFORMAT") and self._parse_string(),
                serde=self._match_text_seq("SERDE") and self._parse_string(),
            )
        return self._parse_as_command(self._prev)

    def _parse_delete(self) -> exp.Delete:
        # This handles MySQL's "Multiple-Table Syntax"
        # https://dev.mysql.com/doc/refman/8.0/en/delete.html
        tables = None
        if not self._match(TokenType.FROM, advance=False):
            tables = self._parse_csv(self._parse_table) or None

        returning = self._parse_returning()

        return self.expression(
            exp.Delete,
            tables=tables,
            this=self._match(TokenType.FROM) and self._parse_table(joins=True),
            using=self._match(TokenType.USING) and self._parse_table(joins=True),
            where=self._parse_where(),
            returning=returning or self._parse_returning(),
            limit=self._parse_limit(),
        )

    def _parse_update(self) -> exp.Update:
        this = self._parse_table(alias_tokens=self.UPDATE_ALIAS_TOKENS)
        expressions = self._match(TokenType.SET) and self._parse_csv(self._parse_equality)
        returning = self._parse_returning()
        return self.expression(
            exp.Update,
            **{  # type: ignore
                "this": this,
                "expressions": expressions,
                "from": self._parse_from(joins=True),
                "where": self._parse_where(),
                "returning": returning or self._parse_returning(),
                "limit": self._parse_limit(),
            },
        )

    def _parse_uncache(self) -> exp.Uncache:
        if not self._match(TokenType.TABLE):
            self.raise_error("Expecting TABLE after UNCACHE")

        return self.expression(
            exp.Uncache, exists=self._parse_exists(), this=self._parse_table(schema=True)
        )

    def _parse_cache(self) -> exp.Cache:
        lazy = self._match_text_seq("LAZY")
        self._match(TokenType.TABLE)
        table = self._parse_table(schema=True)

        options = []
        if self._match_text_seq("OPTIONS"):
            self._match_l_paren()
            k = self._parse_string()
            self._match(TokenType.EQ)
            v = self._parse_string()
            options = [k, v]
            self._match_r_paren()

        self._match(TokenType.ALIAS)
        return self.expression(
            exp.Cache,
            this=table,
            lazy=lazy,
            options=options,
            expression=self._parse_select(nested=True),
        )

    def _parse_partition(self) -> t.Optional[exp.Partition]:
        if not self._match(TokenType.PARTITION):
            return None

        return self.expression(
            exp.Partition, expressions=self._parse_wrapped_csv(self._parse_conjunction)
        )

    def _parse_value(self) -> exp.Tuple:
        if self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(self._parse_conjunction)
            self._match_r_paren()
            return self.expression(exp.Tuple, expressions=expressions)

        # In presto we can have VALUES 1, 2 which results in 1 column & 2 rows.
        # https://prestodb.io/docs/current/sql/values.html
        return self.expression(exp.Tuple, expressions=[self._parse_conjunction()])

    def _parse_select(
        self, nested: bool = False, table: bool = False, parse_subquery_alias: bool = True
    ) -> t.Optional[exp.Expression]:
        cte = self._parse_with()
        if cte:
            this = self._parse_statement()

            if not this:
                self.raise_error("Failed to parse any statement following CTE")
                return cte

            if "with" in this.arg_types:
                this.set("with", cte)
            else:
                self.raise_error(f"{this.key} does not support CTE")
                this = cte
        elif self._match(TokenType.SELECT):
            comments = self._prev_comments

            hint = self._parse_hint()
            all_ = self._match(TokenType.ALL)
            distinct = self._match(TokenType.DISTINCT)

            kind = (
                self._match(TokenType.ALIAS)
                and self._match_texts(("STRUCT", "VALUE"))
                and self._prev.text
            )

            if distinct:
                distinct = self.expression(
                    exp.Distinct,
                    on=self._parse_value() if self._match(TokenType.ON) else None,
                )

            if all_ and distinct:
                self.raise_error("Cannot specify both ALL and DISTINCT after SELECT")

            limit = self._parse_limit(top=True)
            expressions = self._parse_expressions()

            this = self.expression(
                exp.Select,
                kind=kind,
                hint=hint,
                distinct=distinct,
                expressions=expressions,
                limit=limit,
            )
            this.comments = comments

            into = self._parse_into()
            if into:
                this.set("into", into)

            from_ = self._parse_from()
            if from_:
                this.set("from", from_)

            this = self._parse_query_modifiers(this)
        elif (table or nested) and self._match(TokenType.L_PAREN):
            if self._match(TokenType.PIVOT):
                this = self._parse_simplified_pivot()
            elif self._match(TokenType.FROM):
                this = exp.select("*").from_(
                    t.cast(exp.From, self._parse_from(skip_from_token=True))
                )
            else:
                this = self._parse_table() if table else self._parse_select(nested=True)
                this = self._parse_set_operations(self._parse_query_modifiers(this))

            self._match_r_paren()

            # We return early here so that the UNION isn't attached to the subquery by the
            # following call to _parse_set_operations, but instead becomes the parent node
            return self._parse_subquery(this, parse_alias=parse_subquery_alias)
        elif self._match(TokenType.VALUES):
            this = self.expression(
                exp.Values,
                expressions=self._parse_csv(self._parse_value),
                alias=self._parse_table_alias(),
            )
        else:
            this = None

        return self._parse_set_operations(this)

    def _parse_with(self, skip_with_token: bool = False) -> t.Optional[exp.With]:
        if not skip_with_token and not self._match(TokenType.WITH):
            return None

        comments = self._prev_comments
        recursive = self._match(TokenType.RECURSIVE)

        expressions = []
        while True:
            expressions.append(self._parse_cte())

            if not self._match(TokenType.COMMA) and not self._match(TokenType.WITH):
                break
            else:
                self._match(TokenType.WITH)

        return self.expression(
            exp.With, comments=comments, expressions=expressions, recursive=recursive
        )

    def _parse_cte(self) -> exp.CTE:
        alias = self._parse_table_alias()
        if not alias or not alias.this:
            self.raise_error("Expected CTE to have alias")

        self._match(TokenType.ALIAS)
        return self.expression(
            exp.CTE, this=self._parse_wrapped(self._parse_statement), alias=alias
        )

    def _parse_table_alias(
        self, alias_tokens: t.Optional[t.Collection[TokenType]] = None
    ) -> t.Optional[exp.TableAlias]:
        any_token = self._match(TokenType.ALIAS)
        alias = (
            self._parse_id_var(any_token=any_token, tokens=alias_tokens or self.TABLE_ALIAS_TOKENS)
            or self._parse_string_as_identifier()
        )

        index = self._index
        if self._match(TokenType.L_PAREN):
            columns = self._parse_csv(self._parse_function_parameter)
            self._match_r_paren() if columns else self._retreat(index)
        else:
            columns = None

        if not alias and not columns:
            return None

        return self.expression(exp.TableAlias, this=alias, columns=columns)

    def _parse_subquery(
        self, this: t.Optional[exp.Expression], parse_alias: bool = True
    ) -> t.Optional[exp.Subquery]:
        if not this:
            return None

        return self.expression(
            exp.Subquery,
            this=this,
            pivots=self._parse_pivots(),
            alias=self._parse_table_alias() if parse_alias else None,
        )

    def _parse_query_modifiers(
        self, this: t.Optional[exp.Expression]
    ) -> t.Optional[exp.Expression]:
        if isinstance(this, self.MODIFIABLES):
            for join in iter(self._parse_join, None):
                this.append("joins", join)
            for lateral in iter(self._parse_lateral, None):
                this.append("laterals", lateral)

            while True:
                if self._match_set(self.QUERY_MODIFIER_PARSERS, advance=False):
                    parser = self.QUERY_MODIFIER_PARSERS[self._curr.token_type]
                    key, expression = parser(self)

                    if expression:
                        this.set(key, expression)
                        if key == "limit":
                            offset = expression.args.pop("offset", None)
                            if offset:
                                this.set("offset", exp.Offset(expression=offset))
                        continue
                break
        return this

    def _parse_hint(self) -> t.Optional[exp.Hint]:
        if self._match(TokenType.HINT):
            hints = []
            for hint in iter(lambda: self._parse_csv(self._parse_function), []):
                hints.extend(hint)

            if not self._match_pair(TokenType.STAR, TokenType.SLASH):
                self.raise_error("Expected */ after HINT")

            return self.expression(exp.Hint, expressions=hints)

        return None

    def _parse_into(self) -> t.Optional[exp.Into]:
        if not self._match(TokenType.INTO):
            return None

        temp = self._match(TokenType.TEMPORARY)
        unlogged = self._match_text_seq("UNLOGGED")
        self._match(TokenType.TABLE)

        return self.expression(
            exp.Into, this=self._parse_table(schema=True), temporary=temp, unlogged=unlogged
        )

    def _parse_from(
        self, joins: bool = False, skip_from_token: bool = False
    ) -> t.Optional[exp.From]:
        if not skip_from_token and not self._match(TokenType.FROM):
            return None

        return self.expression(
            exp.From, comments=self._prev_comments, this=self._parse_table(joins=joins)
        )

    def _parse_match_recognize(self) -> t.Optional[exp.MatchRecognize]:
        if not self._match(TokenType.MATCH_RECOGNIZE):
            return None

        self._match_l_paren()

        partition = self._parse_partition_by()
        order = self._parse_order()
        measures = self._parse_expressions() if self._match_text_seq("MEASURES") else None

        if self._match_text_seq("ONE", "ROW", "PER", "MATCH"):
            rows = exp.var("ONE ROW PER MATCH")
        elif self._match_text_seq("ALL", "ROWS", "PER", "MATCH"):
            text = "ALL ROWS PER MATCH"
            if self._match_text_seq("SHOW", "EMPTY", "MATCHES"):
                text += f" SHOW EMPTY MATCHES"
            elif self._match_text_seq("OMIT", "EMPTY", "MATCHES"):
                text += f" OMIT EMPTY MATCHES"
            elif self._match_text_seq("WITH", "UNMATCHED", "ROWS"):
                text += f" WITH UNMATCHED ROWS"
            rows = exp.var(text)
        else:
            rows = None

        if self._match_text_seq("AFTER", "MATCH", "SKIP"):
            text = "AFTER MATCH SKIP"
            if self._match_text_seq("PAST", "LAST", "ROW"):
                text += f" PAST LAST ROW"
            elif self._match_text_seq("TO", "NEXT", "ROW"):
                text += f" TO NEXT ROW"
            elif self._match_text_seq("TO", "FIRST"):
                text += f" TO FIRST {self._advance_any().text}"  # type: ignore
            elif self._match_text_seq("TO", "LAST"):
                text += f" TO LAST {self._advance_any().text}"  # type: ignore
            after = exp.var(text)
        else:
            after = None

        if self._match_text_seq("PATTERN"):
            self._match_l_paren()

            if not self._curr:
                self.raise_error("Expecting )", self._curr)

            paren = 1
            start = self._curr

            while self._curr and paren > 0:
                if self._curr.token_type == TokenType.L_PAREN:
                    paren += 1
                if self._curr.token_type == TokenType.R_PAREN:
                    paren -= 1

                end = self._prev
                self._advance()

            if paren > 0:
                self.raise_error("Expecting )", self._curr)

            pattern = exp.var(self._find_sql(start, end))
        else:
            pattern = None

        define = (
            self._parse_csv(
                lambda: self.expression(
                    exp.Alias,
                    alias=self._parse_id_var(any_token=True),
                    this=self._match(TokenType.ALIAS) and self._parse_conjunction(),
                )
            )
            if self._match_text_seq("DEFINE")
            else None
        )

        self._match_r_paren()

        return self.expression(
            exp.MatchRecognize,
            partition_by=partition,
            order=order,
            measures=measures,
            rows=rows,
            after=after,
            pattern=pattern,
            define=define,
            alias=self._parse_table_alias(),
        )

    def _parse_lateral(self) -> t.Optional[exp.Lateral]:
        outer_apply = self._match_pair(TokenType.OUTER, TokenType.APPLY)
        cross_apply = self._match_pair(TokenType.CROSS, TokenType.APPLY)

        if outer_apply or cross_apply:
            this = self._parse_select(table=True)
            view = None
            outer = not cross_apply
        elif self._match(TokenType.LATERAL):
            this = self._parse_select(table=True)
            view = self._match(TokenType.VIEW)
            outer = self._match(TokenType.OUTER)
        else:
            return None

        if not this:
            this = self._parse_function() or self._parse_id_var(any_token=False)
            while self._match(TokenType.DOT):
                this = exp.Dot(
                    this=this,
                    expression=self._parse_function() or self._parse_id_var(any_token=False),
                )

        if view:
            table = self._parse_id_var(any_token=False)
            columns = self._parse_csv(self._parse_id_var) if self._match(TokenType.ALIAS) else []
            table_alias: t.Optional[exp.TableAlias] = self.expression(
                exp.TableAlias, this=table, columns=columns
            )
        elif isinstance(this, exp.Subquery) and this.alias:
            # Ensures parity between the Subquery's and the Lateral's "alias" args
            table_alias = this.args["alias"].copy()
        else:
            table_alias = self._parse_table_alias()

        return self.expression(exp.Lateral, this=this, view=view, outer=outer, alias=table_alias)

    def _parse_join_parts(
        self,
    ) -> t.Tuple[t.Optional[Token], t.Optional[Token], t.Optional[Token]]:
        return (
            self._match_set(self.JOIN_METHODS) and self._prev,
            self._match_set(self.JOIN_SIDES) and self._prev,
            self._match_set(self.JOIN_KINDS) and self._prev,
        )

    def _parse_join(
        self, skip_join_token: bool = False, parse_bracket: bool = False
    ) -> t.Optional[exp.Join]:
        if self._match(TokenType.COMMA):
            return self.expression(exp.Join, this=self._parse_table())

        index = self._index
        method, side, kind = self._parse_join_parts()
        hint = self._prev.text if self._match_texts(self.JOIN_HINTS) else None
        join = self._match(TokenType.JOIN)

        if not skip_join_token and not join:
            self._retreat(index)
            kind = None
            method = None
            side = None

        outer_apply = self._match_pair(TokenType.OUTER, TokenType.APPLY, False)
        cross_apply = self._match_pair(TokenType.CROSS, TokenType.APPLY, False)

        if not skip_join_token and not join and not outer_apply and not cross_apply:
            return None

        if outer_apply:
            side = Token(TokenType.LEFT, "LEFT")

        kwargs: t.Dict[str, t.Any] = {"this": self._parse_table(parse_bracket=parse_bracket)}

        if method:
            kwargs["method"] = method.text
        if side:
            kwargs["side"] = side.text
        if kind:
            kwargs["kind"] = kind.text
        if hint:
            kwargs["hint"] = hint

        if self._match(TokenType.ON):
            kwargs["on"] = self._parse_conjunction()
        elif self._match(TokenType.USING):
            kwargs["using"] = self._parse_wrapped_id_vars()
        elif not (kind and kind.token_type == TokenType.CROSS):
            index = self._index
            joins = self._parse_joins()

            if joins and self._match(TokenType.ON):
                kwargs["on"] = self._parse_conjunction()
            elif joins and self._match(TokenType.USING):
                kwargs["using"] = self._parse_wrapped_id_vars()
            else:
                joins = None
                self._retreat(index)

            kwargs["this"].set("joins", joins)

        return self.expression(exp.Join, **kwargs)

    def _parse_index(
        self,
        index: t.Optional[exp.Expression] = None,
    ) -> t.Optional[exp.Index]:
        if index:
            unique = None
            primary = None
            amp = None

            self._match(TokenType.ON)
            self._match(TokenType.TABLE)  # hive
            table = self._parse_table_parts(schema=True)
        else:
            unique = self._match(TokenType.UNIQUE)
            primary = self._match_text_seq("PRIMARY")
            amp = self._match_text_seq("AMP")

            if not self._match(TokenType.INDEX):
                return None

            index = self._parse_id_var()
            table = None

        using = self._parse_field() if self._match(TokenType.USING) else None

        if self._match(TokenType.L_PAREN, advance=False):
            columns = self._parse_wrapped_csv(self._parse_ordered)
        else:
            columns = None

        return self.expression(
            exp.Index,
            this=index,
            table=table,
            using=using,
            columns=columns,
            unique=unique,
            primary=primary,
            amp=amp,
            partition_by=self._parse_partition_by(),
        )

    def _parse_table_hints(self) -> t.Optional[t.List[exp.Expression]]:
        hints: t.List[exp.Expression] = []
        if self._match_pair(TokenType.WITH, TokenType.L_PAREN):
            # https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16
            hints.append(
                self.expression(
                    exp.WithTableHint,
                    expressions=self._parse_csv(
                        lambda: self._parse_function() or self._parse_var(any_token=True)
                    ),
                )
            )
            self._match_r_paren()
        else:
            # https://dev.mysql.com/doc/refman/8.0/en/index-hints.html
            while self._match_set(self.TABLE_INDEX_HINT_TOKENS):
                hint = exp.IndexTableHint(this=self._prev.text.upper())

                self._match_texts({"INDEX", "KEY"})
                if self._match(TokenType.FOR):
                    hint.set("target", self._advance_any() and self._prev.text.upper())

                hint.set("expressions", self._parse_wrapped_id_vars())
                hints.append(hint)

        return hints or None

    def _parse_table_part(self, schema: bool = False) -> t.Optional[exp.Expression]:
        return (
            (not schema and self._parse_function(optional_parens=False))
            or self._parse_id_var(any_token=False)
            or self._parse_string_as_identifier()
            or self._parse_placeholder()
        )

    def _parse_table_parts(self, schema: bool = False) -> exp.Table:
        catalog = None
        db = None
        table = self._parse_table_part(schema=schema)

        while self._match(TokenType.DOT):
            if catalog:
                # This allows nesting the table in arbitrarily many dot expressions if needed
                table = self.expression(
                    exp.Dot, this=table, expression=self._parse_table_part(schema=schema)
                )
            else:
                catalog = db
                db = table
                table = self._parse_table_part(schema=schema)

        if not table:
            self.raise_error(f"Expected table name but got {self._curr}")

        return self.expression(
            exp.Table, this=table, db=db, catalog=catalog, pivots=self._parse_pivots()
        )

    def _parse_table(
        self,
        schema: bool = False,
        joins: bool = False,
        alias_tokens: t.Optional[t.Collection[TokenType]] = None,
        parse_bracket: bool = False,
    ) -> t.Optional[exp.Expression]:
        lateral = self._parse_lateral()
        if lateral:
            return lateral

        unnest = self._parse_unnest()
        if unnest:
            return unnest

        values = self._parse_derived_table_values()
        if values:
            return values

        subquery = self._parse_select(table=True)
        if subquery:
            if not subquery.args.get("pivots"):
                subquery.set("pivots", self._parse_pivots())
            return subquery

        bracket = parse_bracket and self._parse_bracket(None)
        bracket = self.expression(exp.Table, this=bracket) if bracket else None
        this: exp.Expression = bracket or self._parse_table_parts(schema=schema)

        if schema:
            return self._parse_schema(this=this)

        if self.ALIAS_POST_TABLESAMPLE:
            table_sample = self._parse_table_sample()

        alias = self._parse_table_alias(alias_tokens=alias_tokens or self.TABLE_ALIAS_TOKENS)
        if alias:
            this.set("alias", alias)

        if not this.args.get("pivots"):
            this.set("pivots", self._parse_pivots())

        this.set("hints", self._parse_table_hints())

        if not self.ALIAS_POST_TABLESAMPLE:
            table_sample = self._parse_table_sample()

        if table_sample:
            table_sample.set("this", this)
            this = table_sample

        if joins:
            for join in iter(self._parse_join, None):
                this.append("joins", join)

        return this

    def _parse_unnest(self, with_alias: bool = True) -> t.Optional[exp.Unnest]:
        if not self._match(TokenType.UNNEST):
            return None

        expressions = self._parse_wrapped_csv(self._parse_type)
        ordinality = self._match_pair(TokenType.WITH, TokenType.ORDINALITY)

        alias = self._parse_table_alias() if with_alias else None

        if alias and self.UNNEST_COLUMN_ONLY:
            if alias.args.get("columns"):
                self.raise_error("Unexpected extra column alias in unnest.")

            alias.set("columns", [alias.this])
            alias.set("this", None)

        offset = None
        if self._match_pair(TokenType.WITH, TokenType.OFFSET):
            self._match(TokenType.ALIAS)
            offset = self._parse_id_var() or exp.to_identifier("offset")

        return self.expression(
            exp.Unnest, expressions=expressions, ordinality=ordinality, alias=alias, offset=offset
        )

    def _parse_derived_table_values(self) -> t.Optional[exp.Values]:
        is_derived = self._match_pair(TokenType.L_PAREN, TokenType.VALUES)
        if not is_derived and not self._match(TokenType.VALUES):
            return None

        expressions = self._parse_csv(self._parse_value)
        alias = self._parse_table_alias()

        if is_derived:
            self._match_r_paren()

        return self.expression(
            exp.Values, expressions=expressions, alias=alias or self._parse_table_alias()
        )

    def _parse_table_sample(self, as_modifier: bool = False) -> t.Optional[exp.TableSample]:
        if not self._match(TokenType.TABLE_SAMPLE) and not (
            as_modifier and self._match_text_seq("USING", "SAMPLE")
        ):
            return None

        bucket_numerator = None
        bucket_denominator = None
        bucket_field = None
        percent = None
        rows = None
        size = None
        seed = None

        kind = (
            self._prev.text if self._prev.token_type == TokenType.TABLE_SAMPLE else "USING SAMPLE"
        )
        method = self._parse_var(tokens=(TokenType.ROW,))

        self._match(TokenType.L_PAREN)

        num = self._parse_number()

        if self._match_text_seq("BUCKET"):
            bucket_numerator = self._parse_number()
            self._match_text_seq("OUT", "OF")
            bucket_denominator = bucket_denominator = self._parse_number()
            self._match(TokenType.ON)
            bucket_field = self._parse_field()
        elif self._match_set((TokenType.PERCENT, TokenType.MOD)):
            percent = num
        elif self._match(TokenType.ROWS):
            rows = num
        else:
            size = num

        self._match(TokenType.R_PAREN)

        if self._match(TokenType.L_PAREN):
            method = self._parse_var()
            seed = self._match(TokenType.COMMA) and self._parse_number()
            self._match_r_paren()
        elif self._match_texts(("SEED", "REPEATABLE")):
            seed = self._parse_wrapped(self._parse_number)

        return self.expression(
            exp.TableSample,
            method=method,
            bucket_numerator=bucket_numerator,
            bucket_denominator=bucket_denominator,
            bucket_field=bucket_field,
            percent=percent,
            rows=rows,
            size=size,
            seed=seed,
            kind=kind,
        )

    def _parse_pivots(self) -> t.Optional[t.List[exp.Pivot]]:
        return list(iter(self._parse_pivot, None)) or None

    def _parse_joins(self) -> t.Optional[t.List[exp.Join]]:
        return list(iter(self._parse_join, None)) or None

    # https://duckdb.org/docs/sql/statements/pivot
    def _parse_simplified_pivot(self) -> exp.Pivot:
        def _parse_on() -> t.Optional[exp.Expression]:
            this = self._parse_bitwise()
            return self._parse_in(this) if self._match(TokenType.IN) else this

        this = self._parse_table()
        expressions = self._match(TokenType.ON) and self._parse_csv(_parse_on)
        using = self._match(TokenType.USING) and self._parse_csv(
            lambda: self._parse_alias(self._parse_function())
        )
        group = self._parse_group()
        return self.expression(
            exp.Pivot, this=this, expressions=expressions, using=using, group=group
        )

    def _parse_pivot(self) -> t.Optional[exp.Pivot]:
        index = self._index

        if self._match(TokenType.PIVOT):
            unpivot = False
        elif self._match(TokenType.UNPIVOT):
            unpivot = True
        else:
            return None

        expressions = []
        field = None

        if not self._match(TokenType.L_PAREN):
            self._retreat(index)
            return None

        if unpivot:
            expressions = self._parse_csv(self._parse_column)
        else:
            expressions = self._parse_csv(lambda: self._parse_alias(self._parse_function()))

        if not expressions:
            self.raise_error("Failed to parse PIVOT's aggregation list")

        if not self._match(TokenType.FOR):
            self.raise_error("Expecting FOR")

        value = self._parse_column()

        if not self._match(TokenType.IN):
            self.raise_error("Expecting IN")

        field = self._parse_in(value, alias=True)

        self._match_r_paren()

        pivot = self.expression(exp.Pivot, expressions=expressions, field=field, unpivot=unpivot)

        if not self._match_set((TokenType.PIVOT, TokenType.UNPIVOT), advance=False):
            pivot.set("alias", self._parse_table_alias())

        if not unpivot:
            names = self._pivot_column_names(t.cast(t.List[exp.Expression], expressions))

            columns: t.List[exp.Expression] = []
            for fld in pivot.args["field"].expressions:
                field_name = fld.sql() if self.IDENTIFY_PIVOT_STRINGS else fld.alias_or_name
                for name in names:
                    if self.PREFIXED_PIVOT_COLUMNS:
                        name = f"{name}_{field_name}" if name else field_name
                    else:
                        name = f"{field_name}_{name}" if name else field_name

                    columns.append(exp.to_identifier(name))

            pivot.set("columns", columns)

        return pivot

    def _pivot_column_names(self, aggregations: t.List[exp.Expression]) -> t.List[str]:
        return [agg.alias for agg in aggregations]

    def _parse_where(self, skip_where_token: bool = False) -> t.Optional[exp.Where]:
        if not skip_where_token and not self._match(TokenType.WHERE):
            return None

        return self.expression(
            exp.Where, comments=self._prev_comments, this=self._parse_conjunction()
        )

    def _parse_group(self, skip_group_by_token: bool = False) -> t.Optional[exp.Group]:
        if not skip_group_by_token and not self._match(TokenType.GROUP_BY):
            return None

        elements = defaultdict(list)

        if self._match(TokenType.ALL):
            return self.expression(exp.Group, all=True)

        while True:
            expressions = self._parse_csv(self._parse_conjunction)
            if expressions:
                elements["expressions"].extend(expressions)

            grouping_sets = self._parse_grouping_sets()
            if grouping_sets:
                elements["grouping_sets"].extend(grouping_sets)

            rollup = None
            cube = None
            totals = None

            with_ = self._match(TokenType.WITH)
            if self._match(TokenType.ROLLUP):
                rollup = with_ or self._parse_wrapped_csv(self._parse_column)
                elements["rollup"].extend(ensure_list(rollup))

            if self._match(TokenType.CUBE):
                cube = with_ or self._parse_wrapped_csv(self._parse_column)
                elements["cube"].extend(ensure_list(cube))

            if self._match_text_seq("TOTALS"):
                totals = True
                elements["totals"] = True  # type: ignore

            if not (grouping_sets or rollup or cube or totals):
                break

        return self.expression(exp.Group, **elements)  # type: ignore

    def _parse_grouping_sets(self) -> t.Optional[t.List[t.Optional[exp.Expression]]]:
        if not self._match(TokenType.GROUPING_SETS):
            return None

        return self._parse_wrapped_csv(self._parse_grouping_set)

    def _parse_grouping_set(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.L_PAREN):
            grouping_set = self._parse_csv(self._parse_column)
            self._match_r_paren()
            return self.expression(exp.Tuple, expressions=grouping_set)

        return self._parse_column()

    def _parse_having(self, skip_having_token: bool = False) -> t.Optional[exp.Having]:
        if not skip_having_token and not self._match(TokenType.HAVING):
            return None
        return self.expression(exp.Having, this=self._parse_conjunction())

    def _parse_qualify(self) -> t.Optional[exp.Qualify]:
        if not self._match(TokenType.QUALIFY):
            return None
        return self.expression(exp.Qualify, this=self._parse_conjunction())

    def _parse_order(
        self, this: t.Optional[exp.Expression] = None, skip_order_token: bool = False
    ) -> t.Optional[exp.Expression]:
        if not skip_order_token and not self._match(TokenType.ORDER_BY):
            return this

        return self.expression(
            exp.Order, this=this, expressions=self._parse_csv(self._parse_ordered)
        )

    def _parse_sort(self, exp_class: t.Type[E], token: TokenType) -> t.Optional[E]:
        if not self._match(token):
            return None
        return self.expression(exp_class, expressions=self._parse_csv(self._parse_ordered))

    def _parse_ordered(self) -> exp.Ordered:
        this = self._parse_conjunction()
        self._match(TokenType.ASC)

        is_desc = self._match(TokenType.DESC)
        is_nulls_first = self._match_text_seq("NULLS", "FIRST")
        is_nulls_last = self._match_text_seq("NULLS", "LAST")
        desc = is_desc or False
        asc = not desc
        nulls_first = is_nulls_first or False
        explicitly_null_ordered = is_nulls_first or is_nulls_last

        if (
            not explicitly_null_ordered
            and (
                (asc and self.NULL_ORDERING == "nulls_are_small")
                or (desc and self.NULL_ORDERING != "nulls_are_small")
            )
            and self.NULL_ORDERING != "nulls_are_last"
        ):
            nulls_first = True

        return self.expression(exp.Ordered, this=this, desc=desc, nulls_first=nulls_first)

    def _parse_limit(
        self, this: t.Optional[exp.Expression] = None, top: bool = False
    ) -> t.Optional[exp.Expression]:
        if self._match(TokenType.TOP if top else TokenType.LIMIT):
            comments = self._prev_comments
            if top:
                limit_paren = self._match(TokenType.L_PAREN)
                expression = self._parse_number()

                if limit_paren:
                    self._match_r_paren()
            else:
                expression = self._parse_term()

            if self._match(TokenType.COMMA):
                offset = expression
                expression = self._parse_term()
            else:
                offset = None

            limit_exp = self.expression(
                exp.Limit, this=this, expression=expression, offset=offset, comments=comments
            )

            return limit_exp

        if self._match(TokenType.FETCH):
            direction = self._match_set((TokenType.FIRST, TokenType.NEXT))
            direction = self._prev.text if direction else "FIRST"

            count = self._parse_number()
            percent = self._match(TokenType.PERCENT)

            self._match_set((TokenType.ROW, TokenType.ROWS))

            only = self._match_text_seq("ONLY")
            with_ties = self._match_text_seq("WITH", "TIES")

            if only and with_ties:
                self.raise_error("Cannot specify both ONLY and WITH TIES in FETCH clause")

            return self.expression(
                exp.Fetch,
                direction=direction,
                count=count,
                percent=percent,
                with_ties=with_ties,
            )

        return this

    def _parse_offset(self, this: t.Optional[exp.Expression] = None) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.OFFSET):
            return this

        count = self._parse_term()
        self._match_set((TokenType.ROW, TokenType.ROWS))
        return self.expression(exp.Offset, this=this, expression=count)

    def _parse_locks(self) -> t.List[exp.Lock]:
        locks = []
        while True:
            if self._match_text_seq("FOR", "UPDATE"):
                update = True
            elif self._match_text_seq("FOR", "SHARE") or self._match_text_seq(
                "LOCK", "IN", "SHARE", "MODE"
            ):
                update = False
            else:
                break

            expressions = None
            if self._match_text_seq("OF"):
                expressions = self._parse_csv(lambda: self._parse_table(schema=True))

            wait: t.Optional[bool | exp.Expression] = None
            if self._match_text_seq("NOWAIT"):
                wait = True
            elif self._match_text_seq("WAIT"):
                wait = self._parse_primary()
            elif self._match_text_seq("SKIP", "LOCKED"):
                wait = False

            locks.append(
                self.expression(exp.Lock, update=update, expressions=expressions, wait=wait)
            )

        return locks

    def _parse_set_operations(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if not self._match_set(self.SET_OPERATIONS):
            return this

        token_type = self._prev.token_type

        if token_type == TokenType.UNION:
            expression = exp.Union
        elif token_type == TokenType.EXCEPT:
            expression = exp.Except
        else:
            expression = exp.Intersect

        return self.expression(
            expression,
            this=this,
            distinct=self._match(TokenType.DISTINCT) or not self._match(TokenType.ALL),
            expression=self._parse_set_operations(self._parse_select(nested=True)),
        )

    def _parse_expression(self) -> t.Optional[exp.Expression]:
        return self._parse_alias(self._parse_conjunction())

    def _parse_conjunction(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_equality, self.CONJUNCTION)

    def _parse_equality(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_comparison, self.EQUALITY)

    def _parse_comparison(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_range, self.COMPARISON)

    def _parse_range(self) -> t.Optional[exp.Expression]:
        this = self._parse_bitwise()
        negate = self._match(TokenType.NOT)

        if self._match_set(self.RANGE_PARSERS):
            expression = self.RANGE_PARSERS[self._prev.token_type](self, this)
            if not expression:
                return this

            this = expression
        elif self._match(TokenType.ISNULL):
            this = self.expression(exp.Is, this=this, expression=exp.Null())

        # Postgres supports ISNULL and NOTNULL for conditions.
        # https://blog.andreiavram.ro/postgresql-null-composite-type/
        if self._match(TokenType.NOTNULL):
            this = self.expression(exp.Is, this=this, expression=exp.Null())
            this = self.expression(exp.Not, this=this)

        if negate:
            this = self.expression(exp.Not, this=this)

        if self._match(TokenType.IS):
            this = self._parse_is(this)

        return this

    def _parse_is(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        index = self._index - 1
        negate = self._match(TokenType.NOT)

        if self._match_text_seq("DISTINCT", "FROM"):
            klass = exp.NullSafeEQ if negate else exp.NullSafeNEQ
            return self.expression(klass, this=this, expression=self._parse_expression())

        expression = self._parse_null() or self._parse_boolean()
        if not expression:
            self._retreat(index)
            return None

        this = self.expression(exp.Is, this=this, expression=expression)
        return self.expression(exp.Not, this=this) if negate else this

    def _parse_in(self, this: t.Optional[exp.Expression], alias: bool = False) -> exp.In:
        unnest = self._parse_unnest(with_alias=False)
        if unnest:
            this = self.expression(exp.In, this=this, unnest=unnest)
        elif self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(lambda: self._parse_select_or_expression(alias=alias))

            if len(expressions) == 1 and isinstance(expressions[0], exp.Subqueryable):
                this = self.expression(exp.In, this=this, query=expressions[0])
            else:
                this = self.expression(exp.In, this=this, expressions=expressions)

            self._match_r_paren(this)
        else:
            this = self.expression(exp.In, this=this, field=self._parse_field())

        return this

    def _parse_between(self, this: exp.Expression) -> exp.Between:
        low = self._parse_bitwise()
        self._match(TokenType.AND)
        high = self._parse_bitwise()
        return self.expression(exp.Between, this=this, low=low, high=high)

    def _parse_escape(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.ESCAPE):
            return this
        return self.expression(exp.Escape, this=this, expression=self._parse_string())

    def _parse_interval(self) -> t.Optional[exp.Interval]:
        if not self._match(TokenType.INTERVAL):
            return None

        if self._match(TokenType.STRING, advance=False):
            this = self._parse_primary()
        else:
            this = self._parse_term()

        unit = self._parse_function() or self._parse_var()

        # Most dialects support, e.g., the form INTERVAL '5' day, thus we try to parse
        # each INTERVAL expression into this canonical form so it's easy to transpile
        if this and this.is_number:
            this = exp.Literal.string(this.name)
        elif this and this.is_string:
            parts = this.name.split()

            if len(parts) == 2:
                if unit:
                    # this is not actually a unit, it's something else
                    unit = None
                    self._retreat(self._index - 1)
                else:
                    this = exp.Literal.string(parts[0])
                    unit = self.expression(exp.Var, this=parts[1])

        return self.expression(exp.Interval, this=this, unit=unit)

    def _parse_bitwise(self) -> t.Optional[exp.Expression]:
        this = self._parse_term()

        while True:
            if self._match_set(self.BITWISE):
                this = self.expression(
                    self.BITWISE[self._prev.token_type], this=this, expression=self._parse_term()
                )
            elif self._match_pair(TokenType.LT, TokenType.LT):
                this = self.expression(
                    exp.BitwiseLeftShift, this=this, expression=self._parse_term()
                )
            elif self._match_pair(TokenType.GT, TokenType.GT):
                this = self.expression(
                    exp.BitwiseRightShift, this=this, expression=self._parse_term()
                )
            else:
                break

        return this

    def _parse_term(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_factor, self.TERM)

    def _parse_factor(self) -> t.Optional[exp.Expression]:
        return self._parse_tokens(self._parse_unary, self.FACTOR)

    def _parse_unary(self) -> t.Optional[exp.Expression]:
        if self._match_set(self.UNARY_PARSERS):
            return self.UNARY_PARSERS[self._prev.token_type](self)
        return self._parse_at_time_zone(self._parse_type())

    def _parse_type(self) -> t.Optional[exp.Expression]:
        interval = self._parse_interval()
        if interval:
            return interval

        index = self._index
        data_type = self._parse_types(check_func=True)
        this = self._parse_column()

        if data_type:
            if isinstance(this, exp.Literal):
                parser = self.TYPE_LITERAL_PARSERS.get(data_type.this)
                if parser:
                    return parser(self, this, data_type)
                return self.expression(exp.Cast, this=this, to=data_type)
            if not data_type.expressions:
                self._retreat(index)
                return self._parse_column()
            return self._parse_column_ops(data_type)

        return this

    def _parse_type_size(self) -> t.Optional[exp.DataTypeSize]:
        this = self._parse_type()
        if not this:
            return None

        return self.expression(
            exp.DataTypeSize, this=this, expression=self._parse_var(any_token=True)
        )

    def _parse_types(
        self, check_func: bool = False, schema: bool = False
    ) -> t.Optional[exp.Expression]:
        index = self._index

        prefix = self._match_text_seq("SYSUDTLIB", ".")

        if not self._match_set(self.TYPE_TOKENS):
            return None

        type_token = self._prev.token_type

        if type_token == TokenType.PSEUDO_TYPE:
            return self.expression(exp.PseudoType, this=self._prev.text)

        nested = type_token in self.NESTED_TYPE_TOKENS
        is_struct = type_token == TokenType.STRUCT
        expressions = None
        maybe_func = False

        if self._match(TokenType.L_PAREN):
            if is_struct:
                expressions = self._parse_csv(self._parse_struct_types)
            elif nested:
                expressions = self._parse_csv(
                    lambda: self._parse_types(check_func=check_func, schema=schema)
                )
            elif type_token in self.ENUM_TYPE_TOKENS:
                expressions = self._parse_csv(self._parse_primary)
            else:
                expressions = self._parse_csv(self._parse_type_size)

            if not expressions or not self._match(TokenType.R_PAREN):
                self._retreat(index)
                return None

            maybe_func = True

        if self._match_pair(TokenType.L_BRACKET, TokenType.R_BRACKET):
            this = exp.DataType(
                this=exp.DataType.Type.ARRAY,
                expressions=[
                    exp.DataType(
                        this=exp.DataType.Type[type_token.value],
                        expressions=expressions,
                        nested=nested,
                    )
                ],
                nested=True,
            )

            while self._match_pair(TokenType.L_BRACKET, TokenType.R_BRACKET):
                this = exp.DataType(this=exp.DataType.Type.ARRAY, expressions=[this], nested=True)

            return this

        if self._match(TokenType.L_BRACKET):
            self._retreat(index)
            return None

        values: t.Optional[t.List[t.Optional[exp.Expression]]] = None
        if nested and self._match(TokenType.LT):
            if is_struct:
                expressions = self._parse_csv(self._parse_struct_types)
            else:
                expressions = self._parse_csv(
                    lambda: self._parse_types(check_func=check_func, schema=schema)
                )

            if not self._match(TokenType.GT):
                self.raise_error("Expecting >")

            if self._match_set((TokenType.L_BRACKET, TokenType.L_PAREN)):
                values = self._parse_csv(self._parse_conjunction)
                self._match_set((TokenType.R_BRACKET, TokenType.R_PAREN))

        value: t.Optional[exp.Expression] = None
        if type_token in self.TIMESTAMPS:
            if self._match_text_seq("WITH", "TIME", "ZONE"):
                maybe_func = False
                value = exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ, expressions=expressions)
            elif self._match_text_seq("WITH", "LOCAL", "TIME", "ZONE"):
                maybe_func = False
                value = exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ, expressions=expressions)
            elif self._match_text_seq("WITHOUT", "TIME", "ZONE"):
                maybe_func = False
        elif type_token == TokenType.INTERVAL:
            unit = self._parse_var()

            if not unit:
                value = self.expression(exp.DataType, this=exp.DataType.Type.INTERVAL)
            else:
                value = self.expression(exp.Interval, unit=unit)

        if maybe_func and check_func:
            index2 = self._index
            peek = self._parse_string()

            if not peek:
                self._retreat(index)
                return None

            self._retreat(index2)

        if value:
            return value

        return exp.DataType(
            this=exp.DataType.Type[type_token.value],
            expressions=expressions,
            nested=nested,
            values=values,
            prefix=prefix,
        )

    def _parse_struct_types(self) -> t.Optional[exp.Expression]:
        this = self._parse_type() or self._parse_id_var()
        self._match(TokenType.COLON)
        return self._parse_column_def(this)

    def _parse_at_time_zone(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if not self._match_text_seq("AT", "TIME", "ZONE"):
            return this
        return self.expression(exp.AtTimeZone, this=this, zone=self._parse_unary())

    def _parse_column(self) -> t.Optional[exp.Expression]:
        this = self._parse_field()
        if isinstance(this, exp.Identifier):
            this = self.expression(exp.Column, this=this)
        elif not this:
            return self._parse_bracket(this)
        return self._parse_column_ops(this)

    def _parse_column_ops(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        this = self._parse_bracket(this)

        while self._match_set(self.COLUMN_OPERATORS):
            op_token = self._prev.token_type
            op = self.COLUMN_OPERATORS.get(op_token)

            if op_token == TokenType.DCOLON:
                field = self._parse_types()
                if not field:
                    self.raise_error("Expected type")
            elif op and self._curr:
                self._advance()
                value = self._prev.text
                field = (
                    exp.Literal.number(value)
                    if self._prev.token_type == TokenType.NUMBER
                    else exp.Literal.string(value)
                )
            else:
                field = self._parse_field(anonymous_func=True, any_token=True)

            if isinstance(field, exp.Func):
                # bigquery allows function calls like x.y.count(...)
                # SAFE.SUBSTR(...)
                # https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-reference#function_call_rules
                this = self._replace_columns_with_dots(this)

            if op:
                this = op(self, this, field)
            elif isinstance(this, exp.Column) and not this.args.get("catalog"):
                this = self.expression(
                    exp.Column,
                    this=field,
                    table=this.this,
                    db=this.args.get("table"),
                    catalog=this.args.get("db"),
                )
            else:
                this = self.expression(exp.Dot, this=this, expression=field)
            this = self._parse_bracket(this)
        return this

    def _parse_primary(self) -> t.Optional[exp.Expression]:
        if self._match_set(self.PRIMARY_PARSERS):
            token_type = self._prev.token_type
            primary = self.PRIMARY_PARSERS[token_type](self, self._prev)

            if token_type == TokenType.STRING:
                expressions = [primary]
                while self._match(TokenType.STRING):
                    expressions.append(exp.Literal.string(self._prev.text))

                if len(expressions) > 1:
                    return self.expression(exp.Concat, expressions=expressions)

            return primary

        if self._match_pair(TokenType.DOT, TokenType.NUMBER):
            return exp.Literal.number(f"0.{self._prev.text}")

        if self._match(TokenType.L_PAREN):
            comments = self._prev_comments
            query = self._parse_select()

            if query:
                expressions = [query]
            else:
                expressions = self._parse_expressions()

            this = self._parse_query_modifiers(seq_get(expressions, 0))

            if isinstance(this, exp.Subqueryable):
                this = self._parse_set_operations(
                    self._parse_subquery(this=this, parse_alias=False)
                )
            elif len(expressions) > 1:
                this = self.expression(exp.Tuple, expressions=expressions)
            else:
                this = self.expression(exp.Paren, this=self._parse_set_operations(this))

            if this:
                this.add_comments(comments)

            self._match_r_paren(expression=this)
            return this

        return None

    def _parse_field(
        self,
        any_token: bool = False,
        tokens: t.Optional[t.Collection[TokenType]] = None,
        anonymous_func: bool = False,
    ) -> t.Optional[exp.Expression]:
        return (
            self._parse_primary()
            or self._parse_function(anonymous=anonymous_func)
            or self._parse_id_var(any_token=any_token, tokens=tokens)
        )

    def _parse_function(
        self,
        functions: t.Optional[t.Dict[str, t.Callable]] = None,
        anonymous: bool = False,
        optional_parens: bool = True,
    ) -> t.Optional[exp.Expression]:
        if not self._curr:
            return None

        token_type = self._curr.token_type

        if optional_parens and self._match_set(self.NO_PAREN_FUNCTION_PARSERS):
            return self.NO_PAREN_FUNCTION_PARSERS[token_type](self)

        if not self._next or self._next.token_type != TokenType.L_PAREN:
            if optional_parens and token_type in self.NO_PAREN_FUNCTIONS:
                self._advance()
                return self.expression(self.NO_PAREN_FUNCTIONS[token_type])

            return None

        if token_type not in self.FUNC_TOKENS:
            return None

        this = self._curr.text
        upper = this.upper()
        self._advance(2)

        parser = self.FUNCTION_PARSERS.get(upper)

        if parser and not anonymous:
            this = parser(self)
        else:
            subquery_predicate = self.SUBQUERY_PREDICATES.get(token_type)

            if subquery_predicate and self._curr.token_type in (TokenType.SELECT, TokenType.WITH):
                this = self.expression(subquery_predicate, this=self._parse_select())
                self._match_r_paren()
                return this

            if functions is None:
                functions = self.FUNCTIONS

            function = functions.get(upper)

            alias = upper in self.FUNCTIONS_WITH_ALIASED_ARGS
            args = self._parse_csv(lambda: self._parse_lambda(alias=alias))

            if function and not anonymous:
                this = self.validate_expression(function(args), args)
            else:
                this = self.expression(exp.Anonymous, this=this, expressions=args)

        self._match(TokenType.R_PAREN, expression=this)
        return self._parse_window(this)

    def _parse_function_parameter(self) -> t.Optional[exp.Expression]:
        return self._parse_column_def(self._parse_id_var())

    def _parse_user_defined_function(
        self, kind: t.Optional[TokenType] = None
    ) -> t.Optional[exp.Expression]:
        this = self._parse_id_var()

        while self._match(TokenType.DOT):
            this = self.expression(exp.Dot, this=this, expression=self._parse_id_var())

        if not self._match(TokenType.L_PAREN):
            return this

        expressions = self._parse_csv(self._parse_function_parameter)
        self._match_r_paren()
        return self.expression(
            exp.UserDefinedFunction, this=this, expressions=expressions, wrapped=True
        )

    def _parse_introducer(self, token: Token) -> exp.Introducer | exp.Identifier:
        literal = self._parse_primary()
        if literal:
            return self.expression(exp.Introducer, this=token.text, expression=literal)

        return self.expression(exp.Identifier, this=token.text)

    def _parse_session_parameter(self) -> exp.SessionParameter:
        kind = None
        this = self._parse_id_var() or self._parse_primary()

        if this and self._match(TokenType.DOT):
            kind = this.name
            this = self._parse_var() or self._parse_primary()

        return self.expression(exp.SessionParameter, this=this, kind=kind)

    def _parse_lambda(self, alias: bool = False) -> t.Optional[exp.Expression]:
        index = self._index

        if self._match(TokenType.L_PAREN):
            expressions = self._parse_csv(self._parse_id_var)

            if not self._match(TokenType.R_PAREN):
                self._retreat(index)
        else:
            expressions = [self._parse_id_var()]

        if self._match_set(self.LAMBDAS):
            return self.LAMBDAS[self._prev.token_type](self, expressions)

        self._retreat(index)

        this: t.Optional[exp.Expression]

        if self._match(TokenType.DISTINCT):
            this = self.expression(
                exp.Distinct, expressions=self._parse_csv(self._parse_conjunction)
            )
        else:
            this = self._parse_select_or_expression(alias=alias)

        return self._parse_limit(self._parse_order(self._parse_respect_or_ignore_nulls(this)))

    def _parse_schema(self, this: t.Optional[exp.Expression] = None) -> t.Optional[exp.Expression]:
        index = self._index

        if not self.errors:
            try:
                if self._parse_select(nested=True):
                    return this
            except ParseError:
                pass
            finally:
                self.errors.clear()
                self._retreat(index)

        if not self._match(TokenType.L_PAREN):
            return this

        args = self._parse_csv(
            lambda: self._parse_constraint()
            or self._parse_column_def(self._parse_field(any_token=True))
        )

        self._match_r_paren()
        return self.expression(exp.Schema, this=this, expressions=args)

    def _parse_column_def(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        # column defs are not really columns, they're identifiers
        if isinstance(this, exp.Column):
            this = this.this

        kind = self._parse_types(schema=True)

        if self._match_text_seq("FOR", "ORDINALITY"):
            return self.expression(exp.ColumnDef, this=this, ordinality=True)

        constraints = []
        while True:
            constraint = self._parse_column_constraint()
            if not constraint:
                break
            constraints.append(constraint)

        if not kind and not constraints:
            return this

        return self.expression(exp.ColumnDef, this=this, kind=kind, constraints=constraints)

    def _parse_auto_increment(
        self,
    ) -> exp.GeneratedAsIdentityColumnConstraint | exp.AutoIncrementColumnConstraint:
        start = None
        increment = None

        if self._match(TokenType.L_PAREN, advance=False):
            args = self._parse_wrapped_csv(self._parse_bitwise)
            start = seq_get(args, 0)
            increment = seq_get(args, 1)
        elif self._match_text_seq("START"):
            start = self._parse_bitwise()
            self._match_text_seq("INCREMENT")
            increment = self._parse_bitwise()

        if start and increment:
            return exp.GeneratedAsIdentityColumnConstraint(start=start, increment=increment)

        return exp.AutoIncrementColumnConstraint()

    def _parse_compress(self) -> exp.CompressColumnConstraint:
        if self._match(TokenType.L_PAREN, advance=False):
            return self.expression(
                exp.CompressColumnConstraint, this=self._parse_wrapped_csv(self._parse_bitwise)
            )

        return self.expression(exp.CompressColumnConstraint, this=self._parse_bitwise())

    def _parse_generated_as_identity(self) -> exp.GeneratedAsIdentityColumnConstraint:
        if self._match_text_seq("BY", "DEFAULT"):
            on_null = self._match_pair(TokenType.ON, TokenType.NULL)
            this = self.expression(
                exp.GeneratedAsIdentityColumnConstraint, this=False, on_null=on_null
            )
        else:
            self._match_text_seq("ALWAYS")
            this = self.expression(exp.GeneratedAsIdentityColumnConstraint, this=True)

        self._match(TokenType.ALIAS)
        identity = self._match_text_seq("IDENTITY")

        if self._match(TokenType.L_PAREN):
            if self._match_text_seq("START", "WITH"):
                this.set("start", self._parse_bitwise())
            if self._match_text_seq("INCREMENT", "BY"):
                this.set("increment", self._parse_bitwise())
            if self._match_text_seq("MINVALUE"):
                this.set("minvalue", self._parse_bitwise())
            if self._match_text_seq("MAXVALUE"):
                this.set("maxvalue", self._parse_bitwise())

            if self._match_text_seq("CYCLE"):
                this.set("cycle", True)
            elif self._match_text_seq("NO", "CYCLE"):
                this.set("cycle", False)

            if not identity:
                this.set("expression", self._parse_bitwise())

            self._match_r_paren()

        return this

    def _parse_inline(self) -> exp.InlineLengthColumnConstraint:
        self._match_text_seq("LENGTH")
        return self.expression(exp.InlineLengthColumnConstraint, this=self._parse_bitwise())

    def _parse_not_constraint(
        self,
    ) -> t.Optional[exp.NotNullColumnConstraint | exp.CaseSpecificColumnConstraint]:
        if self._match_text_seq("NULL"):
            return self.expression(exp.NotNullColumnConstraint)
        if self._match_text_seq("CASESPECIFIC"):
            return self.expression(exp.CaseSpecificColumnConstraint, not_=True)
        return None

    def _parse_column_constraint(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.CONSTRAINT):
            this = self._parse_id_var()
        else:
            this = None

        if self._match_texts(self.CONSTRAINT_PARSERS):
            return self.expression(
                exp.ColumnConstraint,
                this=this,
                kind=self.CONSTRAINT_PARSERS[self._prev.text.upper()](self),
            )

        return this

    def _parse_constraint(self) -> t.Optional[exp.Expression]:
        if not self._match(TokenType.CONSTRAINT):
            return self._parse_unnamed_constraint(constraints=self.SCHEMA_UNNAMED_CONSTRAINTS)

        this = self._parse_id_var()
        expressions = []

        while True:
            constraint = self._parse_unnamed_constraint() or self._parse_function()
            if not constraint:
                break
            expressions.append(constraint)

        return self.expression(exp.Constraint, this=this, expressions=expressions)

    def _parse_unnamed_constraint(
        self, constraints: t.Optional[t.Collection[str]] = None
    ) -> t.Optional[exp.Expression]:
        if not self._match_texts(constraints or self.CONSTRAINT_PARSERS):
            return None

        constraint = self._prev.text.upper()
        if constraint not in self.CONSTRAINT_PARSERS:
            self.raise_error(f"No parser found for schema constraint {constraint}.")

        return self.CONSTRAINT_PARSERS[constraint](self)

    def _parse_unique(self) -> exp.UniqueColumnConstraint:
        self._match_text_seq("KEY")
        return self.expression(
            exp.UniqueColumnConstraint, this=self._parse_schema(self._parse_id_var(any_token=False))
        )

    def _parse_key_constraint_options(self) -> t.List[str]:
        options = []
        while True:
            if not self._curr:
                break

            if self._match(TokenType.ON):
                action = None
                on = self._advance_any() and self._prev.text

                if self._match_text_seq("NO", "ACTION"):
                    action = "NO ACTION"
                elif self._match_text_seq("CASCADE"):
                    action = "CASCADE"
                elif self._match_pair(TokenType.SET, TokenType.NULL):
                    action = "SET NULL"
                elif self._match_pair(TokenType.SET, TokenType.DEFAULT):
                    action = "SET DEFAULT"
                else:
                    self.raise_error("Invalid key constraint")

                options.append(f"ON {on} {action}")
            elif self._match_text_seq("NOT", "ENFORCED"):
                options.append("NOT ENFORCED")
            elif self._match_text_seq("DEFERRABLE"):
                options.append("DEFERRABLE")
            elif self._match_text_seq("INITIALLY", "DEFERRED"):
                options.append("INITIALLY DEFERRED")
            elif self._match_text_seq("NORELY"):
                options.append("NORELY")
            elif self._match_text_seq("MATCH", "FULL"):
                options.append("MATCH FULL")
            else:
                break

        return options

    def _parse_references(self, match: bool = True) -> t.Optional[exp.Reference]:
        if match and not self._match(TokenType.REFERENCES):
            return None

        expressions = None
        this = self._parse_table(schema=True)
        options = self._parse_key_constraint_options()
        return self.expression(exp.Reference, this=this, expressions=expressions, options=options)

    def _parse_foreign_key(self) -> exp.ForeignKey:
        expressions = self._parse_wrapped_id_vars()
        reference = self._parse_references()
        options = {}

        while self._match(TokenType.ON):
            if not self._match_set((TokenType.DELETE, TokenType.UPDATE)):
                self.raise_error("Expected DELETE or UPDATE")

            kind = self._prev.text.lower()

            if self._match_text_seq("NO", "ACTION"):
                action = "NO ACTION"
            elif self._match(TokenType.SET):
                self._match_set((TokenType.NULL, TokenType.DEFAULT))
                action = "SET " + self._prev.text.upper()
            else:
                self._advance()
                action = self._prev.text.upper()

            options[kind] = action

        return self.expression(
            exp.ForeignKey, expressions=expressions, reference=reference, **options  # type: ignore
        )

    def _parse_primary_key(
        self, wrapped_optional: bool = False, in_props: bool = False
    ) -> exp.PrimaryKeyColumnConstraint | exp.PrimaryKey:
        desc = (
            self._match_set((TokenType.ASC, TokenType.DESC))
            and self._prev.token_type == TokenType.DESC
        )

        if not in_props and not self._match(TokenType.L_PAREN, advance=False):
            return self.expression(exp.PrimaryKeyColumnConstraint, desc=desc)

        expressions = self._parse_wrapped_csv(self._parse_field, optional=wrapped_optional)
        options = self._parse_key_constraint_options()
        return self.expression(exp.PrimaryKey, expressions=expressions, options=options)

    def _parse_bracket(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if not self._match_set((TokenType.L_BRACKET, TokenType.L_BRACE)):
            return this

        bracket_kind = self._prev.token_type

        if self._match(TokenType.COLON):
            expressions: t.List[t.Optional[exp.Expression]] = [
                self.expression(exp.Slice, expression=self._parse_conjunction())
            ]
        else:
            expressions = self._parse_csv(lambda: self._parse_slice(self._parse_conjunction()))

        # https://duckdb.org/docs/sql/data_types/struct.html#creating-structs
        if bracket_kind == TokenType.L_BRACE:
            this = self.expression(exp.Struct, expressions=expressions)
        elif not this or this.name.upper() == "ARRAY":
            this = self.expression(exp.Array, expressions=expressions)
        else:
            expressions = apply_index_offset(this, expressions, -self.INDEX_OFFSET)
            this = self.expression(exp.Bracket, this=this, expressions=expressions)

        if not self._match(TokenType.R_BRACKET) and bracket_kind == TokenType.L_BRACKET:
            self.raise_error("Expected ]")
        elif not self._match(TokenType.R_BRACE) and bracket_kind == TokenType.L_BRACE:
            self.raise_error("Expected }")

        self._add_comments(this)
        return self._parse_bracket(this)

    def _parse_slice(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
        if self._match(TokenType.COLON):
            return self.expression(exp.Slice, this=this, expression=self._parse_conjunction())
        return this

    def _parse_case(self) -> t.Optional[exp.Expression]:
        ifs = []
        default = None

        expression = self._parse_conjunction()

        while self._match(TokenType.WHEN):
            this = self._parse_conjunction()
            self._match(TokenType.THEN)
            then = self._parse_conjunction()
            ifs.append(self.expression(exp.If, this=this, true=then))

        if self._match(TokenType.ELSE):
            default = self._parse_conjunction()

        if not self._match(TokenType.END):
            self.raise_error("Expected END after CASE", self._prev)

        return self._parse_window(
            self.expression(exp.Case, this=expression, ifs=ifs, default=default)
        )

    def _parse_if(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.L_PAREN):
            args = self._parse_csv(self._parse_conjunction)
            this = self.validate_expression(exp.If.from_arg_list(args), args)
            self._match_r_paren()
        else:
            index = self._index - 1
            condition = self._parse_conjunction()

            if not condition:
                self._retreat(index)
                return None

            self._match(TokenType.THEN)
            true = self._parse_conjunction()
            false = self._parse_conjunction() if self._match(TokenType.ELSE) else None
            self._match(TokenType.END)
            this = self.expression(exp.If, this=condition, true=true, false=false)

        return self._parse_window(this)

    def _parse_extract(self) -> exp.Extract:
        this = self._parse_function() or self._parse_var() or self._parse_type()

        if self._match(TokenType.FROM):
            return self.expression(exp.Extract, this=this, expression=self._parse_bitwise())

        if not self._match(TokenType.COMMA):
            self.raise_error("Expected FROM or comma after EXTRACT", self._prev)

        return self.expression(exp.Extract, this=this, expression=self._parse_bitwise())

    def _parse_any_value(self) -> exp.AnyValue:
        this = self._parse_lambda()
        is_max = None
        having = None

        if self._match(TokenType.HAVING):
            self._match_texts(("MAX", "MIN"))
            is_max = self._prev.text == "MAX"
            having = self._parse_column()

        return self.expression(exp.AnyValue, this=this, having=having, max=is_max)

    def _parse_cast(self, strict: bool) -> exp.Expression:
        this = self._parse_conjunction()

        if not self._match(TokenType.ALIAS):
            if self._match(TokenType.COMMA):
                return self.expression(
                    exp.CastToStrType, this=this, expression=self._parse_string()
                )
            else:
                self.raise_error("Expected AS after CAST")

        fmt = None
        to = self._parse_types()

        if not to:
            self.raise_error("Expected TYPE after CAST")
        elif to.this == exp.DataType.Type.CHAR:
            if self._match(TokenType.CHARACTER_SET):
                to = self.expression(exp.CharacterSet, this=self._parse_var_or_string())
        elif self._match(TokenType.FORMAT):
            fmt_string = self._parse_string()
            fmt = self._parse_at_time_zone(fmt_string)

            if to.this in exp.DataType.TEMPORAL_TYPES:
                this = self.expression(
                    exp.StrToDate if to.this == exp.DataType.Type.DATE else exp.StrToTime,
                    this=this,
                    format=exp.Literal.string(
                        format_time(
                            fmt_string.this if fmt_string else "",
                            self.FORMAT_MAPPING or self.TIME_MAPPING,
                            self.FORMAT_TRIE or self.TIME_TRIE,
                        )
                    ),
                )

                if isinstance(fmt, exp.AtTimeZone) and isinstance(this, exp.StrToTime):
                    this.set("zone", fmt.args["zone"])

                return this

        return self.expression(exp.Cast if strict else exp.TryCast, this=this, to=to, format=fmt)

    def _parse_concat(self) -> t.Optional[exp.Expression]:
        args = self._parse_csv(self._parse_conjunction)
        if self.CONCAT_NULL_OUTPUTS_STRING:
            args = [
                exp.func("COALESCE", exp.cast(arg, "text"), exp.Literal.string(""))
                for arg in args
                if arg
            ]

        # Some dialects (e.g. Trino) don't allow a single-argument CONCAT call, so when
        # we find such a call we replace it with its argument.
        if len(args) == 1:
            return args[0]

        return self.expression(
            exp.Concat if self.STRICT_STRING_CONCAT else exp.SafeConcat, expressions=args
        )

    def _parse_string_agg(self) -> exp.Expression:
        if self._match(TokenType.DISTINCT):
            args: t.List[t.Optional[exp.Expression]] = [
                self.expression(exp.Distinct, expressions=[self._parse_conjunction()])
            ]
            if self._match(TokenType.COMMA):
                args.extend(self._parse_csv(self._parse_conjunction))
        else:
            args = self._parse_csv(self._parse_conjunction)

        index = self._index
        if not self._match(TokenType.R_PAREN):
            # postgres: STRING_AGG([DISTINCT] expression, separator [ORDER BY expression1 {ASC | DESC} [, ...]])
            return self.expression(
                exp.GroupConcat,
                this=seq_get(args, 0),
                separator=self._parse_order(this=seq_get(args, 1)),
            )

        # Checks if we can parse an order clause: WITHIN GROUP (ORDER BY <order_by_expression_list> [ASC | DESC]).
        # This is done "manually", instead of letting _parse_window parse it into an exp.WithinGroup node, so that
        # the STRING_AGG call is parsed like in MySQL / SQLite and can thus be transpiled more easily to them.
        if not self._match_text_seq("WITHIN", "GROUP"):
            self._retreat(index)
            return self.validate_expression(exp.GroupConcat.from_arg_list(args), args)

        self._match_l_paren()  # The corresponding match_r_paren will be called in parse_function (caller)
        order = self._parse_order(this=seq_get(args, 0))
        return self.expression(exp.GroupConcat, this=order, separator=seq_get(args, 1))

    def _parse_convert(self, strict: bool) -> t.Optional[exp.Expression]:
        this = self._parse_bitwise()

        if self._match(TokenType.USING):
            to: t.Optional[exp.Expression] = self.expression(
                exp.CharacterSet, this=self._parse_var()
            )
        elif self._match(TokenType.COMMA):
            to = self._parse_types()
        else:
            to = None

        return self.expression(exp.Cast if strict else exp.TryCast, this=this, to=to)

    def _parse_decode(self) -> t.Optional[exp.Decode | exp.Case]:
        """
        There are generally two variants of the DECODE function:

        - DECODE(bin, charset)
        - DECODE(expression, search, result [, search, result] ... [, default])

        The second variant will always be parsed into a CASE expression. Note that NULL
        needs special treatment, since we need to explicitly check for it with `IS NULL`,
        instead of relying on pattern matching.
        """
        args = self._parse_csv(self._parse_conjunction)

        if len(args) < 3:
            return self.expression(exp.Decode, this=seq_get(args, 0), charset=seq_get(args, 1))

        expression, *expressions = args
        if not expression:
            return None

        ifs = []
        for search, result in zip(expressions[::2], expressions[1::2]):
            if not search or not result:
                return None

            if isinstance(search, exp.Literal):
                ifs.append(
                    exp.If(this=exp.EQ(this=expression.copy(), expression=search), true=result)
                )
            elif isinstance(search, exp.Null):
                ifs.append(
                    exp.If(this=exp.Is(this=expression.copy(), expression=exp.Null()), true=result)
                )
            else:
                cond = exp.or_(
                    exp.EQ(this=expression.copy(), expression=search),
                    exp.and_(
                        exp.Is(this=expression.copy(), expression=exp.Null()),
                        exp.Is(this=search.copy(), expression=exp.Null()),
                        copy=False,
                    ),
                    copy=False,
                )
                ifs.append(exp.If(this=cond, true=result))

        return exp.Case(ifs=ifs, default=expressions[-1] if len(expressions) % 2 == 1 else None)

    def _parse_json_key_value(self) -> t.Optional[exp.JSONKeyValue]:
        self._match_text_seq("KEY")
        key = self._parse_field()
        self._match(TokenType.COLON)
        self._match_text_seq("VALUE")
        value = self._parse_field()

        if not key and not value:
            return None
        return self.expression(exp.JSONKeyValue, this=key, expression=value)

    def _parse_json_object(self) -> exp.JSONObject:
        star = self._parse_star()
        expressions = [star] if star else self._parse_csv(self._parse_json_key_value)

        null_handling = None
        if self._match_text_seq("NULL", "ON", "NULL"):
            null_handling = "NULL ON NULL"
        elif self._match_text_seq("ABSENT", "ON", "NULL"):
            null_handling = "ABSENT ON NULL"

        unique_keys = None
        if self._match_text_seq("WITH", "UNIQUE"):
            unique_keys = True
        elif self._match_text_seq("WITHOUT", "UNIQUE"):
            unique_keys = False

        self._match_text_seq("KEYS")

        return_type = self._match_text_seq("RETURNING") and self._parse_type()
        format_json = self._match_text_seq("FORMAT", "JSON")
        encoding = self._match_text_seq("ENCODING") and self._parse_var()

        return self.expression(
            exp.JSONObject,
            expressions=expressions,
            null_handling=null_handling,
            unique_keys=unique_keys,
            return_type=return_type,
            format_json=format_json,
            encoding=encoding,
        )

    def _parse_logarithm(self) -> exp.Func:
        # Default argument order is base, expression
        args = self._parse_csv(self._parse_range)

        if len(args) > 1:
            if not self.LOG_BASE_FIRST:
                args.reverse()
            return exp.Log.from_arg_list(args)

        return self.expression(
            exp.Ln if self.LOG_DEFAULTS_TO_LN else exp.Log, this=seq_get(args, 0)
        )

    def _parse_match_against(self) -> exp.MatchAgainst:
        expressions = self._parse_csv(self._parse_column)

        self._match_text_seq(")", "AGAINST", "(")

        this = self._parse_string()

        if self._match_text_seq("IN", "NATURAL", "LANGUAGE", "MODE"):
            modifier = "IN NATURAL LANGUAGE MODE"
            if self._match_text_seq("WITH", "QUERY", "EXPANSION"):
                modifier = f"{modifier} WITH QUERY EXPANSION"
        elif self._match_text_seq("IN", "BOOLEAN", "MODE"):
            modifier = "IN BOOLEAN MODE"
        elif self._match_text_seq("WITH", "QUERY", "EXPANSION"):
            modifier = "WITH QUERY EXPANSION"
        else:
            modifier = None

        return self.expression(
            exp.MatchAgainst, this=this, expressions=expressions, modifier=modifier
        )

    # https://learn.microsoft.com/en-us/sql/t-sql/functions/openjson-transact-sql?view=sql-server-ver16
    def _parse_open_json(self) -> exp.OpenJSON:
        this = self._parse_bitwise()
        path = self._match(TokenType.COMMA) and self._parse_string()

        def _parse_open_json_column_def() -> exp.OpenJSONColumnDef:
            this = self._parse_field(any_token=True)
            kind = self._parse_types()
            path = self._parse_string()
            as_json = self._match_pair(TokenType.ALIAS, TokenType.JSON)

            return self.expression(
                exp.OpenJSONColumnDef, this=this, kind=kind, path=path, as_json=as_json
            )

        expressions = None
        if self._match_pair(TokenType.R_PAREN, TokenType.WITH):
            self._match_l_paren()
            expressions = self._parse_csv(_parse_open_json_column_def)

        return self.expression(exp.OpenJSON, this=this, path=path, expressions=expressions)

    def _parse_position(self, haystack_first: bool = False) -> exp.StrPosition:
        args = self._parse_csv(self._parse_bitwise)

        if self._match(TokenType.IN):
            return self.expression(
                exp.StrPosition, this=self._parse_bitwise(), substr=seq_get(args, 0)
            )

        if haystack_first:
            haystack = seq_get(args, 0)
            needle = seq_get(args, 1)
        else:
            needle = seq_get(args, 0)
            haystack = seq_get(args, 1)

        return self.expression(
            exp.StrPosition, this=haystack, substr=needle, position=seq_get(args, 2)
        )

    def _parse_join_hint(self, func_name: str) -> exp.JoinHint:
        args = self._parse_csv(self._parse_table)
        return exp.JoinHint(this=func_name.upper(), expressions=args)

    def _parse_substring(self) -> exp.Substring:
        # Postgres supports the form: substring(string [from int] [for int])
        # https://www.postgresql.org/docs/9.1/functions-string.html @ Table 9-6

        args = self._parse_csv(self._parse_bitwise)

        if self._match(TokenType.FROM):
            args.append(self._parse_bitwise())
            if self._match(TokenType.FOR):
                args.append(self._parse_bitwise())

        return self.validate_expression(exp.Substring.from_arg_list(args), args)

    def _parse_trim(self) -> exp.Trim:
        # https://www.w3resource.com/sql/character-functions/trim.php
        # https://docs.oracle.com/javadb/10.8.3.0/ref/rreftrimfunc.html

        position = None
        collation = None

        if self._match_texts(self.TRIM_TYPES):
            position = self._prev.text.upper()

        expression = self._parse_bitwise()
        if self._match_set((TokenType.FROM, TokenType.COMMA)):
            this = self._parse_bitwise()
        else:
            this = expression
            expression = None

        if self._match(TokenType.COLLATE):
            collation = self._parse_bitwise()

        return self.expression(
            exp.Trim, this=this, position=position, expression=expression, collation=collation
        )

    def _parse_window_clause(self) -> t.Optional[t.List[t.Optional[exp.Expression]]]:
        return self._match(TokenType.WINDOW) and self._parse_csv(self._parse_named_window)

    def _parse_named_window(self) -> t.Optional[exp.Expression]:
        return self._parse_window(self._parse_id_var(), alias=True)

    def _parse_respect_or_ignore_nulls(
        self, this: t.Optional[exp.Expression]
    ) -> t.Optional[exp.Expression]:
        if self._match_text_seq("IGNORE", "NULLS"):
            return self.expression(exp.IgnoreNulls, this=this)
        if self._match_text_seq("RESPECT", "NULLS"):
            return self.expression(exp.RespectNulls, this=this)
        return this

    def _parse_window(
        self, this: t.Optional[exp.Expression], alias: bool = False
    ) -> t.Optional[exp.Expression]:
        if self._match_pair(TokenType.FILTER, TokenType.L_PAREN):
            self._match(TokenType.WHERE)
            this = self.expression(
                exp.Filter, this=this, expression=self._parse_where(skip_where_token=True)
            )
            self._match_r_paren()

        # T-SQL allows the OVER (...) syntax after WITHIN GROUP.
        # https://learn.microsoft.com/en-us/sql/t-sql/functions/percentile-disc-transact-sql?view=sql-server-ver16
        if self._match_text_seq("WITHIN", "GROUP"):
            order = self._parse_wrapped(self._parse_order)
            this = self.expression(exp.WithinGroup, this=this, expression=order)

        # SQL spec defines an optional [ { IGNORE | RESPECT } NULLS ] OVER
        # Some dialects choose to implement and some do not.
        # https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html

        # There is some code above in _parse_lambda that handles
        #   SELECT FIRST_VALUE(TABLE.COLUMN IGNORE|RESPECT NULLS) OVER ...

        # The below changes handle
        #   SELECT FIRST_VALUE(TABLE.COLUMN) IGNORE|RESPECT NULLS OVER ...

        # Oracle allows both formats
        #   (https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/img_text/first_value.html)
        #   and Snowflake chose to do the same for familiarity
        #   https://docs.snowflake.com/en/sql-reference/functions/first_value.html#usage-notes
        this = self._parse_respect_or_ignore_nulls(this)

        # bigquery select from window x AS (partition by ...)
        if alias:
            over = None
            self._match(TokenType.ALIAS)
        elif not self._match_set(self.WINDOW_BEFORE_PAREN_TOKENS):
            return this
        else:
            over = self._prev.text.upper()

        if not self._match(TokenType.L_PAREN):
            return self.expression(
                exp.Window, this=this, alias=self._parse_id_var(False), over=over
            )

        window_alias = self._parse_id_var(any_token=False, tokens=self.WINDOW_ALIAS_TOKENS)

        first = self._match(TokenType.FIRST)
        if self._match_text_seq("LAST"):
            first = False

        partition = self._parse_partition_by()
        order = self._parse_order()
        kind = self._match_set((TokenType.ROWS, TokenType.RANGE)) and self._prev.text

        if kind:
            self._match(TokenType.BETWEEN)
            start = self._parse_window_spec()
            self._match(TokenType.AND)
            end = self._parse_window_spec()

            spec = self.expression(
                exp.WindowSpec,
                kind=kind,
                start=start["value"],
                start_side=start["side"],
                end=end["value"],
                end_side=end["side"],
            )
        else:
            spec = None

        self._match_r_paren()

        return self.expression(
            exp.Window,
            this=this,
            partition_by=partition,
            order=order,
            spec=spec,
            alias=window_alias,
            over=over,
            first=first,
        )

    def _parse_window_spec(self) -> t.Dict[str, t.Optional[str | exp.Expression]]:
        self._match(TokenType.BETWEEN)

        return {
            "value": (
                (self._match_text_seq("UNBOUNDED") and "UNBOUNDED")
                or (self._match_text_seq("CURRENT", "ROW") and "CURRENT ROW")
                or self._parse_bitwise()
            ),
            "side": self._match_texts(self.WINDOW_SIDES) and self._prev.text,
        }

    def _parse_alias(
        self, this: t.Optional[exp.Expression], explicit: bool = False
    ) -> t.Optional[exp.Expression]:
        any_token = self._match(TokenType.ALIAS)

        if explicit and not any_token:
            return this

        if self._match(TokenType.L_PAREN):
            aliases = self.expression(
                exp.Aliases,
                this=this,
                expressions=self._parse_csv(lambda: self._parse_id_var(any_token)),
            )
            self._match_r_paren(aliases)
            return aliases

        alias = self._parse_id_var(any_token)

        if alias:
            return self.expression(exp.Alias, this=this, alias=alias)

        return this

    def _parse_id_var(
        self,
        any_token: bool = True,
        tokens: t.Optional[t.Collection[TokenType]] = None,
    ) -> t.Optional[exp.Expression]:
        identifier = self._parse_identifier()

        if identifier:
            return identifier

        if (any_token and self._advance_any()) or self._match_set(tokens or self.ID_VAR_TOKENS):
            quoted = self._prev.token_type == TokenType.STRING
            return exp.Identifier(this=self._prev.text, quoted=quoted)

        return None

    def _parse_string(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.STRING):
            return self.PRIMARY_PARSERS[TokenType.STRING](self, self._prev)
        return self._parse_placeholder()

    def _parse_string_as_identifier(self) -> t.Optional[exp.Identifier]:
        return exp.to_identifier(self._match(TokenType.STRING) and self._prev.text, quoted=True)

    def _parse_number(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.NUMBER):
            return self.PRIMARY_PARSERS[TokenType.NUMBER](self, self._prev)
        return self._parse_placeholder()

    def _parse_identifier(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.IDENTIFIER):
            return self.expression(exp.Identifier, this=self._prev.text, quoted=True)
        return self._parse_placeholder()

    def _parse_var(
        self, any_token: bool = False, tokens: t.Optional[t.Collection[TokenType]] = None
    ) -> t.Optional[exp.Expression]:
        if (
            (any_token and self._advance_any())
            or self._match(TokenType.VAR)
            or (self._match_set(tokens) if tokens else False)
        ):
            return self.expression(exp.Var, this=self._prev.text)
        return self._parse_placeholder()

    def _advance_any(self) -> t.Optional[Token]:
        if self._curr and self._curr.token_type not in self.RESERVED_KEYWORDS:
            self._advance()
            return self._prev
        return None

    def _parse_var_or_string(self) -> t.Optional[exp.Expression]:
        return self._parse_var() or self._parse_string()

    def _parse_null(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.NULL):
            return self.PRIMARY_PARSERS[TokenType.NULL](self, self._prev)
        return None

    def _parse_boolean(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.TRUE):
            return self.PRIMARY_PARSERS[TokenType.TRUE](self, self._prev)
        if self._match(TokenType.FALSE):
            return self.PRIMARY_PARSERS[TokenType.FALSE](self, self._prev)
        return None

    def _parse_star(self) -> t.Optional[exp.Expression]:
        if self._match(TokenType.STAR):
            return self.PRIMARY_PARSERS[TokenType.STAR](self, self._prev)
        return None

    def _parse_parameter(self) -> exp.Parameter:
        wrapped = self._match(TokenType.L_BRACE)
        this = self._parse_var() or self._parse_identifier() or self._parse_primary()
        self._match(TokenType.R_BRACE)
        return self.expression(exp.Parameter, this=this, wrapped=wrapped)

    def _parse_placeholder(self) -> t.Optional[exp.Expression]:
        if self._match_set(self.PLACEHOLDER_PARSERS):
            placeholder = self.PLACEHOLDER_PARSERS[self._prev.token_type](self)
            if placeholder:
                return placeholder
            self._advance(-1)
        return None

    def _parse_except(self) -> t.Optional[t.List[t.Optional[exp.Expression]]]:
        if not self._match(TokenType.EXCEPT):
            return None
        if self._match(TokenType.L_PAREN, advance=False):
            return self._parse_wrapped_csv(self._parse_column)
        return self._parse_csv(self._parse_column)

    def _parse_replace(self) -> t.Optional[t.List[t.Optional[exp.Expression]]]:
        if not self._match(TokenType.REPLACE):
            return None
        if self._match(TokenType.L_PAREN, advance=False):
            return self._parse_wrapped_csv(self._parse_expression)
        return self._parse_expressions()

    def _parse_csv(
        self, parse_method: t.Callable, sep: TokenType = TokenType.COMMA
    ) -> t.List[t.Optional[exp.Expression]]:
        parse_result = parse_method()
        items = [parse_result] if parse_result is not None else []

        while self._match(sep):
            self._add_comments(parse_result)
            parse_result = parse_method()
            if parse_result is not None:
                items.append(parse_result)

        return items

    def _parse_tokens(
        self, parse_method: t.Callable, expressions: t.Dict
    ) -> t.Optional[exp.Expression]:
        this = parse_method()

        while self._match_set(expressions):
            this = self.expression(
                expressions[self._prev.token_type],
                this=this,
                comments=self._prev_comments,
                expression=parse_method(),
            )

        return this

    def _parse_wrapped_id_vars(self, optional: bool = False) -> t.List[t.Optional[exp.Expression]]:
        return self._parse_wrapped_csv(self._parse_id_var, optional=optional)

    def _parse_wrapped_csv(
        self, parse_method: t.Callable, sep: TokenType = TokenType.COMMA, optional: bool = False
    ) -> t.List[t.Optional[exp.Expression]]:
        return self._parse_wrapped(
            lambda: self._parse_csv(parse_method, sep=sep), optional=optional
        )

    def _parse_wrapped(self, parse_method: t.Callable, optional: bool = False) -> t.Any:
        wrapped = self._match(TokenType.L_PAREN)
        if not wrapped and not optional:
            self.raise_error("Expecting (")
        parse_result = parse_method()
        if wrapped:
            self._match_r_paren()
        return parse_result

    def _parse_expressions(self) -> t.List[t.Optional[exp.Expression]]:
        return self._parse_csv(self._parse_expression)

    def _parse_select_or_expression(self, alias: bool = False) -> t.Optional[exp.Expression]:
        return self._parse_select() or self._parse_set_operations(
            self._parse_expression() if alias else self._parse_conjunction()
        )

    def _parse_ddl_select(self) -> t.Optional[exp.Expression]:
        return self._parse_query_modifiers(
            self._parse_set_operations(self._parse_select(nested=True, parse_subquery_alias=False))
        )

    def _parse_transaction(self) -> exp.Transaction | exp.Command:
        this = None
        if self._match_texts(self.TRANSACTION_KIND):
            this = self._prev.text

        self._match_texts({"TRANSACTION", "WORK"})

        modes = []
        while True:
            mode = []
            while self._match(TokenType.VAR):
                mode.append(self._prev.text)

            if mode:
                modes.append(" ".join(mode))
            if not self._match(TokenType.COMMA):
                break

        return self.expression(exp.Transaction, this=this, modes=modes)

    def _parse_commit_or_rollback(self) -> exp.Commit | exp.Rollback:
        chain = None
        savepoint = None
        is_rollback = self._prev.token_type == TokenType.ROLLBACK

        self._match_texts({"TRANSACTION", "WORK"})

        if self._match_text_seq("TO"):
            self._match_text_seq("SAVEPOINT")
            savepoint = self._parse_id_var()

        if self._match(TokenType.AND):
            chain = not self._match_text_seq("NO")
            self._match_text_seq("CHAIN")

        if is_rollback:
            return self.expression(exp.Rollback, savepoint=savepoint)

        return self.expression(exp.Commit, chain=chain)

    def _parse_add_column(self) -> t.Optional[exp.Expression]:
        if not self._match_text_seq("ADD"):
            return None

        self._match(TokenType.COLUMN)
        exists_column = self._parse_exists(not_=True)
        expression = self._parse_column_def(self._parse_field(any_token=True))

        if expression:
            expression.set("exists", exists_column)

            # https://docs.databricks.com/delta/update-schema.html#explicitly-update-schema-to-add-columns
            if self._match_texts(("FIRST", "AFTER")):
                position = self._prev.text
                column_position = self.expression(
                    exp.ColumnPosition, this=self._parse_column(), position=position
                )
                expression.set("position", column_position)

        return expression

    def _parse_drop_column(self) -> t.Optional[exp.Drop | exp.Command]:
        drop = self._match(TokenType.DROP) and self._parse_drop()
        if drop and not isinstance(drop, exp.Command):
            drop.set("kind", drop.args.get("kind", "COLUMN"))
        return drop

    # https://docs.aws.amazon.com/athena/latest/ug/alter-table-drop-partition.html
    def _parse_drop_partition(self, exists: t.Optional[bool] = None) -> exp.DropPartition:
        return self.expression(
            exp.DropPartition, expressions=self._parse_csv(self._parse_partition), exists=exists
        )

    def _parse_add_constraint(self) -> exp.AddConstraint:
        this = None
        kind = self._prev.token_type

        if kind == TokenType.CONSTRAINT:
            this = self._parse_id_var()

            if self._match_text_seq("CHECK"):
                expression = self._parse_wrapped(self._parse_conjunction)
                enforced = self._match_text_seq("ENFORCED")

                return self.expression(
                    exp.AddConstraint, this=this, expression=expression, enforced=enforced
                )

        if kind == TokenType.FOREIGN_KEY or self._match(TokenType.FOREIGN_KEY):
            expression = self._parse_foreign_key()
        elif kind == TokenType.PRIMARY_KEY or self._match(TokenType.PRIMARY_KEY):
            expression = self._parse_primary_key()
        else:
            expression = None

        return self.expression(exp.AddConstraint, this=this, expression=expression)

    def _parse_alter_table_add(self) -> t.List[t.Optional[exp.Expression]]:
        index = self._index - 1

        if self._match_set(self.ADD_CONSTRAINT_TOKENS):
            return self._parse_csv(self._parse_add_constraint)

        self._retreat(index)
        return self._parse_csv(self._parse_add_column)

    def _parse_alter_table_alter(self) -> exp.AlterColumn:
        self._match(TokenType.COLUMN)
        column = self._parse_field(any_token=True)

        if self._match_pair(TokenType.DROP, TokenType.DEFAULT):
            return self.expression(exp.AlterColumn, this=column, drop=True)
        if self._match_pair(TokenType.SET, TokenType.DEFAULT):
            return self.expression(exp.AlterColumn, this=column, default=self._parse_conjunction())

        self._match_text_seq("SET", "DATA")
        return self.expression(
            exp.AlterColumn,
            this=column,
            dtype=self._match_text_seq("TYPE") and self._parse_types(),
            collate=self._match(TokenType.COLLATE) and self._parse_term(),
            using=self._match(TokenType.USING) and self._parse_conjunction(),
        )

    def _parse_alter_table_drop(self) -> t.List[t.Optional[exp.Expression]]:
        index = self._index - 1

        partition_exists = self._parse_exists()
        if self._match(TokenType.PARTITION, advance=False):
            return self._parse_csv(lambda: self._parse_drop_partition(exists=partition_exists))

        self._retreat(index)
        return self._parse_csv(self._parse_drop_column)

    def _parse_alter_table_rename(self) -> exp.RenameTable:
        self._match_text_seq("TO")
        return self.expression(exp.RenameTable, this=self._parse_table(schema=True))

    def _parse_alter(self) -> exp.AlterTable | exp.Command:
        start = self._prev

        if not self._match(TokenType.TABLE):
            return self._parse_as_command(start)

        exists = self._parse_exists()
        this = self._parse_table(schema=True)

        if self._next:
            self._advance()

        parser = self.ALTER_PARSERS.get(self._prev.text.upper()) if self._prev else None
        if parser:
            actions = ensure_list(parser(self))

            if not self._curr:
                return self.expression(
                    exp.AlterTable,
                    this=this,
                    exists=exists,
                    actions=actions,
                )
        return self._parse_as_command(start)

    def _parse_merge(self) -> exp.Merge:
        self._match(TokenType.INTO)
        target = self._parse_table()

        self._match(TokenType.USING)
        using = self._parse_table()

        self._match(TokenType.ON)
        on = self._parse_conjunction()

        whens = []
        while self._match(TokenType.WHEN):
            matched = not self._match(TokenType.NOT)
            self._match_text_seq("MATCHED")
            source = (
                False
                if self._match_text_seq("BY", "TARGET")
                else self._match_text_seq("BY", "SOURCE")
            )
            condition = self._parse_conjunction() if self._match(TokenType.AND) else None

            self._match(TokenType.THEN)

            if self._match(TokenType.INSERT):
                _this = self._parse_star()
                if _this:
                    then: t.Optional[exp.Expression] = self.expression(exp.Insert, this=_this)
                else:
                    then = self.expression(
                        exp.Insert,
                        this=self._parse_value(),
                        expression=self._match(TokenType.VALUES) and self._parse_value(),
                    )
            elif self._match(TokenType.UPDATE):
                expressions = self._parse_star()
                if expressions:
                    then = self.expression(exp.Update, expressions=expressions)
                else:
                    then = self.expression(
                        exp.Update,
                        expressions=self._match(TokenType.SET)
                        and self._parse_csv(self._parse_equality),
                    )
            elif self._match(TokenType.DELETE):
                then = self.expression(exp.Var, this=self._prev.text)
            else:
                then = None

            whens.append(
                self.expression(
                    exp.When,
                    matched=matched,
                    source=source,
                    condition=condition,
                    then=then,
                )
            )

        return self.expression(
            exp.Merge,
            this=target,
            using=using,
            on=on,
            expressions=whens,
        )

    def _parse_show(self) -> t.Optional[exp.Expression]:
        parser = self._find_parser(self.SHOW_PARSERS, self.SHOW_TRIE)
        if parser:
            return parser(self)
        self._advance()
        return self.expression(exp.Show, this=self._prev.text.upper())

    def _parse_set_item_assignment(
        self, kind: t.Optional[str] = None
    ) -> t.Optional[exp.Expression]:
        index = self._index

        if kind in {"GLOBAL", "SESSION"} and self._match_text_seq("TRANSACTION"):
            return self._parse_set_transaction(global_=kind == "GLOBAL")

        left = self._parse_primary() or self._parse_id_var()

        if not self._match_texts(("=", "TO")):
            self._retreat(index)
            return None

        right = self._parse_statement() or self._parse_id_var()
        this = self.expression(exp.EQ, this=left, expression=right)

        return self.expression(exp.SetItem, this=this, kind=kind)

    def _parse_set_transaction(self, global_: bool = False) -> exp.Expression:
        self._match_text_seq("TRANSACTION")
        characteristics = self._parse_csv(
            lambda: self._parse_var_from_options(self.TRANSACTION_CHARACTERISTICS)
        )
        return self.expression(
            exp.SetItem,
            expressions=characteristics,
            kind="TRANSACTION",
            **{"global": global_},  # type: ignore
        )

    def _parse_set_item(self) -> t.Optional[exp.Expression]:
        parser = self._find_parser(self.SET_PARSERS, self.SET_TRIE)
        return parser(self) if parser else self._parse_set_item_assignment(kind=None)

    def _parse_set(self, unset: bool = False, tag: bool = False) -> exp.Set | exp.Command:
        index = self._index
        set_ = self.expression(
            exp.Set, expressions=self._parse_csv(self._parse_set_item), unset=unset, tag=tag
        )

        if self._curr:
            self._retreat(index)
            return self._parse_as_command(self._prev)

        return set_

    def _parse_var_from_options(self, options: t.Collection[str]) -> t.Optional[exp.Var]:
        for option in options:
            if self._match_text_seq(*option.split(" ")):
                return exp.var(option)
        return None

    def _parse_as_command(self, start: Token) -> exp.Command:
        while self._curr:
            self._advance()
        text = self._find_sql(start, self._prev)
        size = len(start.text)
        return exp.Command(this=text[:size], expression=text[size:])

    def _parse_dict_property(self, this: str) -> exp.DictProperty:
        settings = []

        self._match_l_paren()
        kind = self._parse_id_var()

        if self._match(TokenType.L_PAREN):
            while True:
                key = self._parse_id_var()
                value = self._parse_primary()

                if not key and value is None:
                    break
                settings.append(self.expression(exp.DictSubProperty, this=key, value=value))
            self._match(TokenType.R_PAREN)

        self._match_r_paren()

        return self.expression(
            exp.DictProperty,
            this=this,
            kind=kind.this if kind else None,
            settings=settings,
        )

    def _parse_dict_range(self, this: str) -> exp.DictRange:
        self._match_l_paren()
        has_min = self._match_text_seq("MIN")
        if has_min:
            min = self._parse_var() or self._parse_primary()
            self._match_text_seq("MAX")
            max = self._parse_var() or self._parse_primary()
        else:
            max = self._parse_var() or self._parse_primary()
            min = exp.Literal.number(0)
        self._match_r_paren()
        return self.expression(exp.DictRange, this=this, min=min, max=max)

    def _find_parser(
        self, parsers: t.Dict[str, t.Callable], trie: t.Dict
    ) -> t.Optional[t.Callable]:
        if not self._curr:
            return None

        index = self._index
        this = []
        while True:
            # The current token might be multiple words
            curr = self._curr.text.upper()
            key = curr.split(" ")
            this.append(curr)

            self._advance()
            result, trie = in_trie(trie, key)
            if result == TrieResult.FAILED:
                break

            if result == TrieResult.EXISTS:
                subparser = parsers[" ".join(this)]
                return subparser

        self._retreat(index)
        return None

    def _match(self, token_type, advance=True, expression=None):
        if not self._curr:
            return None

        if self._curr.token_type == token_type:
            if advance:
                self._advance()
            self._add_comments(expression)
            return True

        return None

    def _match_set(self, types, advance=True):
        if not self._curr:
            return None

        if self._curr.token_type in types:
            if advance:
                self._advance()
            return True

        return None

    def _match_pair(self, token_type_a, token_type_b, advance=True):
        if not self._curr or not self._next:
            return None

        if self._curr.token_type == token_type_a and self._next.token_type == token_type_b:
            if advance:
                self._advance(2)
            return True

        return None

    def _match_l_paren(self, expression: t.Optional[exp.Expression] = None) -> None:
        if not self._match(TokenType.L_PAREN, expression=expression):
            self.raise_error("Expecting (")

    def _match_r_paren(self, expression: t.Optional[exp.Expression] = None) -> None:
        if not self._match(TokenType.R_PAREN, expression=expression):
            self.raise_error("Expecting )")

    def _match_texts(self, texts, advance=True):
        if self._curr and self._curr.text.upper() in texts:
            if advance:
                self._advance()
            return True
        return False

    def _match_text_seq(self, *texts, advance=True):
        index = self._index
        for text in texts:
            if self._curr and self._curr.text.upper() == text:
                self._advance()
            else:
                self._retreat(index)
                return False

        if not advance:
            self._retreat(index)

        return True

    @t.overload
    def _replace_columns_with_dots(self, this: exp.Expression) -> exp.Expression:
        ...

    @t.overload
    def _replace_columns_with_dots(
        self, this: t.Optional[exp.Expression]
    ) -> t.Optional[exp.Expression]:
        ...

    def _replace_columns_with_dots(self, this):
        if isinstance(this, exp.Dot):
            exp.replace_children(this, self._replace_columns_with_dots)
        elif isinstance(this, exp.Column):
            exp.replace_children(this, self._replace_columns_with_dots)
            table = this.args.get("table")
            this = (
                self.expression(exp.Dot, this=table, expression=this.this) if table else this.this
            )

        return this

    def _replace_lambda(
        self, node: t.Optional[exp.Expression], lambda_variables: t.Set[str]
    ) -> t.Optional[exp.Expression]:
        if not node:
            return node

        for column in node.find_all(exp.Column):
            if column.parts[0].name in lambda_variables:
                dot_or_id = column.to_dot() if column.table else column.this
                parent = column.parent

                while isinstance(parent, exp.Dot):
                    if not isinstance(parent.parent, exp.Dot):
                        parent.replace(dot_or_id)
                        break
                    parent = parent.parent
                else:
                    if column is node:
                        node = dot_or_id
                    else:
                        column.replace(dot_or_id)
        return node
