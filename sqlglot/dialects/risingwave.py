from __future__ import annotations
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType
from collections import defaultdict
import typing as t

from sqlglot import exp


class RisingWave(Postgres):
    class Tokenizer(Postgres.Tokenizer):
        KEYWORDS = {
            **Postgres.Tokenizer.KEYWORDS,
            "SINK": TokenType.SINK,
            "SOURCE": TokenType.SOURCE,
        }

    class Parser(Postgres.Parser):
        DB_CREATABLES = {
            *Postgres.Parser.DB_CREATABLES,
            TokenType.SINK,
            TokenType.SOURCE,
        }
        CREATABLES = {
            *Postgres.Parser.CREATABLES,
            *DB_CREATABLES,
        }

        def _parse_table_hints(self) -> t.Optional[t.List[exp.Expression]]:
            # There is no hint in risingwave.
            # Do nothing here to avoid WITH keywords conflict in CREATE SINK statement.
            return None

        def _parse_include(self) -> t.Optional[exp.Expression]:
            column_type = self._parse_var_or_string()
            column_alias = None
            inner_field = None
            header_inner_expect_type = None
            if not column_type:
                return None
            if not self._match(TokenType.ALIAS, advance=False) and not self._match_texts(
                "INCLUDE", advance=False
            ):
                inner_field = self._parse_var_or_string()
                if not self._match(TokenType.ALIAS, advance=False) and not self._match_texts(
                    "INCLUDE", advance=False
                ):
                    header_inner_expect_type = self._parse_var_or_string()

            if self._match(TokenType.ALIAS):
                column_alias = self._parse_var_or_string()
            include_property = self.expression(exp.IncludeProperty, column_type=column_type)
            if column_alias:
                include_property.set("column_alias", column_alias)
            if inner_field:
                include_property.set("inner_field", inner_field)
            if header_inner_expect_type:
                include_property.set("header_inner_expect_type", header_inner_expect_type)

            return include_property

        def _parse_encode_property(self, is_key: bool) -> t.Optional[exp.Expression]:
            encode_var = self._parse_var_or_string()
            encode_property: t.Optional[exp.Expression] = self.expression(
                exp.EncodeProperty, this=encode_var, is_key=is_key
            )
            # encode_property: t.Optional[exp.Expression] = self._parse_property_assignment(exp.EncodeProperty)
            expressions = None
            if not encode_property:
                return None
            if self._match(TokenType.L_PAREN, advance=False):
                expressions = exp.Properties(expressions=self._parse_wrapped_properties())
            if expressions:
                encode_property.set("expressions", expressions)
            return encode_property

        def _parse_property(self) -> t.Optional[exp.Expression]:
            if self._match_texts(self.PROPERTY_PARSERS):
                return self.PROPERTY_PARSERS[self._prev.text.upper()](self)

            if self._match(TokenType.DEFAULT) and self._match_texts(self.PROPERTY_PARSERS):
                return self.PROPERTY_PARSERS[self._prev.text.upper()](self, default=True)

            if self._match_text_seq("COMPOUND", "SORTKEY"):
                return self._parse_sortkey(compound=True)

            if self._match_text_seq("SQL", "SECURITY"):
                return self.expression(
                    exp.SqlSecurityProperty, definer=self._match_text_seq("DEFINER")
                )

            # Parse risingwave specific properties.
            if self._match_texts("ENCODE"):
                return self._parse_encode_property(is_key=False)

            if self._match_text_seq("KEY", "ENCODE"):
                return self._parse_encode_property(is_key=True)

            if self._match_texts("INCLUDE"):
                return self._parse_include()

            index = self._index
            key = self._parse_column()

            if not self._match(TokenType.EQ):
                self._retreat(index)
                return self._parse_sequence_properties()

            # Transform the key to exp.Dot if it's dotted identifiers wrapped in exp.Column or to exp.Var otherwise
            if isinstance(key, exp.Column):
                key = key.to_dot() if len(key.parts) > 1 else exp.var(key.name)

            value = self._parse_bitwise() or self._parse_var(any_token=True)

            # Transform the value to exp.Var if it was parsed as exp.Column(exp.Identifier())
            if isinstance(value, exp.Column):
                value = exp.var(value.name)

            return self.expression(exp.Property, this=key, value=value)

        def _parse_create(self) -> exp.Create | exp.Command:
            # Note: this can't be None because we've matched a statement parser
            start = self._prev
            comments = self._prev_comments

            replace = (
                start.token_type == TokenType.REPLACE
                or self._match_pair(TokenType.OR, TokenType.REPLACE)
                or self._match_pair(TokenType.OR, TokenType.ALTER)
            )
            refresh = self._match_pair(TokenType.OR, TokenType.REFRESH)

            unique = self._match(TokenType.UNIQUE)

            if self._match_text_seq("CLUSTERED", "COLUMNSTORE"):
                clustered = True
            elif self._match_text_seq("NONCLUSTERED", "COLUMNSTORE") or self._match_text_seq(
                "COLUMNSTORE"
            ):
                clustered = False
            else:
                clustered = None

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

            concurrently = self._match_text_seq("CONCURRENTLY")
            exists = self._parse_exists(not_=True)
            this = None
            expression: t.Optional[exp.Expression] = None
            indexes = None
            no_schema_binding = None
            begin = None
            end = None
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

                expression = self._match(TokenType.ALIAS) and self._parse_heredoc()
                extend_props(self._parse_properties())

                if not expression:
                    if self._match(TokenType.COMMAND):
                        expression = self._parse_as_command(self._prev)
                    else:
                        begin = self._match(TokenType.BEGIN)
                        return_ = self._match_text_seq("RETURN")

                        if self._match(TokenType.STRING, advance=False):
                            # Takes care of BigQuery's JavaScript UDF definitions that end in an OPTIONS property
                            # # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
                            expression = self._parse_string()
                            extend_props(self._parse_properties())
                        else:
                            expression = self._parse_user_defined_function_expression()

                        end = self._match_text_seq("END")

                        if return_:
                            expression = self.expression(exp.Return, this=expression)
            elif create_token.token_type == TokenType.INDEX:
                # Postgres allows anonymous indexes, eg. CREATE INDEX IF NOT EXISTS ON t(c)
                if not self._match(TokenType.ON):
                    index = self._parse_id_var()
                    anonymous = False
                else:
                    index = None
                    anonymous = True

                this = self._parse_index(index=index, anonymous=anonymous)
            elif create_token.token_type in self.DB_CREATABLES:
                table_parts = self._parse_table_parts(
                    schema=True, is_db_reference=create_token.token_type == TokenType.SCHEMA
                )

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

                if create_token.token_type == TokenType.SEQUENCE:
                    expression = self._parse_types()
                    extend_props(self._parse_properties())
                else:
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
                # Newly added to support Risingwave sink.
                elif create_token.token_type == TokenType.SINK:
                    extend_props(self._parse_properties())
                elif create_token.token_type == TokenType.SOURCE:
                    extend_props(self._parse_properties())

                shallow = self._match_text_seq("SHALLOW")

                if self._match_texts(self.CLONE_KEYWORDS):
                    copy = self._prev.text.lower() == "copy"
                    clone = self.expression(
                        exp.Clone, this=self._parse_table(schema=True), shallow=shallow, copy=copy
                    )

            if self._curr and not self._match_set(
                (TokenType.R_PAREN, TokenType.COMMA), advance=False
            ):
                return self._parse_as_command(start)

            create_kind_text = create_token.text.upper()
            return self.expression(
                exp.Create,
                comments=comments,
                this=this,
                kind=self.dialect.CREATABLE_KIND_MAPPING.get(create_kind_text) or create_kind_text,
                replace=replace,
                refresh=refresh,
                unique=unique,
                expression=expression,
                exists=exists,
                properties=properties,
                indexes=indexes,
                no_schema_binding=no_schema_binding,
                begin=begin,
                end=end,
                clone=clone,
                concurrently=concurrently,
                clustered=clustered,
            )

        def _parse_watermark_field(self) -> t.Optional[exp.Expression]:
            if not self._match_texts("WATERMARK"):
                return None
            if not self._match(TokenType.FOR):
                return None
            column: t.Optional[exp.Expression] = self._parse_var_or_string()
            if not column:
                return None
            if not self._match(TokenType.ALIAS):
                return None
            expr: t.Optional[exp.Expression] = self._parse_expression()
            if not expr:
                return None
            return self.expression(exp.Watermark, column=column, expression=expr)

        def _parse_field_def(self) -> t.Optional[exp.Expression]:
            # First parse if it is watermark field before parsing column def.
            return self._parse_watermark_field() or self._parse_column_def(
                self._parse_field(any_token=True)
            )

        def _parse_column_def(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
            # column defs are not really columns, they're identifiers
            if isinstance(this, exp.Column):
                this = this.this

            kind = self._parse_types(schema=True)

            if self._match_text_seq("FOR", "ORDINALITY"):
                return self.expression(exp.ColumnDef, this=this, ordinality=True)

            constraints: t.List[exp.Expression] = []

            if (not kind and self._match(TokenType.ALIAS)) or self._match_texts(
                ("ALIAS", "MATERIALIZED")
            ):
                persisted = self._prev.text.upper() == "MATERIALIZED"
                constraint_kind = exp.ComputedColumnConstraint(
                    this=self._parse_assignment(),
                    persisted=persisted or self._match_text_seq("PERSISTED"),
                    not_null=self._match_pair(TokenType.NOT, TokenType.NULL),
                )
                constraints.append(self.expression(exp.ColumnConstraint, kind=constraint_kind))
            elif kind and self._match_pair(TokenType.ALIAS, TokenType.L_PAREN, advance=False):
                self._match(TokenType.ALIAS)
                constraints.append(
                    self.expression(
                        exp.ColumnConstraint,
                        kind=exp.TransformColumnConstraint(this=self._parse_field()),
                    )
                )
            elif kind and self._match(TokenType.ALIAS, advance=False):
                # Deal with CREATE SOURCE statement, where we may have col_name data_type [ AS generation_expression ] and generation_expression may not have parentheses, which makes it different compared to previous if.
                self._match(TokenType.ALIAS)
                constraints.append(
                    self.expression(
                        exp.ColumnConstraint,
                        kind=exp.TransformColumnConstraint(this=self._parse_expression()),
                    )
                )

            while True:
                constraint = self._parse_column_constraint()
                if not constraint:
                    break
                constraints.append(constraint)

            if not kind and not constraints:
                return this

            return self.expression(exp.ColumnDef, this=this, kind=kind, constraints=constraints)

    class Generator(Postgres.Generator):
        LOCKING_READS_SUPPORTED = False

        PROPERTIES_LOCATION = {
            **Postgres.Generator.PROPERTIES_LOCATION,
            exp.FileFormatProperty: exp.Properties.Location.POST_EXPRESSION,
            exp.EncodeProperty: exp.Properties.Location.POST_EXPRESSION,
            exp.IncludeProperty: exp.Properties.Location.POST_SCHEMA,
        }

        def _encode_property_sql(self, expression: exp.Expression) -> str:
            is_key = expression.args.get("is_key")
            property_sql_str: str = "KEY ENCODE" if is_key else "ENCODE"

            property_value = self.sql(expression, "this")
            property_sql_str = f"{property_sql_str} {property_value}"

            encode_expr = expression.args.get("expressions")
            if encode_expr:
                property_sql_str = f"{property_sql_str} {self.properties(encode_expr)}"

            return property_sql_str

        def _watermark_sql(self, expression: exp.Expression) -> str:
            column = self.sql(expression, "column")
            expression_str = self.sql(expression, "expression")
            return f"WATERMARK FOR {column} AS {expression_str}"

        def _include_property_sql(self, expression: exp.Expression) -> str:
            column_type = self.sql(expression, "column_type")
            property_sql_str = f"INCLUDE {column_type}"
            inner_field = self.sql(expression, "inner_field")
            if inner_field:
                property_sql_str = f"{property_sql_str} {inner_field}"
            column_alias = self.sql(expression, "column_alias")
            header_inner_expect_type = self.sql(expression, "header_inner_expect_type")
            if header_inner_expect_type:
                property_sql_str = f"{property_sql_str} {header_inner_expect_type}"
            if column_alias:
                property_sql_str = f"{property_sql_str} AS {column_alias}"
            return property_sql_str

        def with_properties(self, properties: exp.Properties) -> str:
            with_sql = self.properties(
                properties, prefix=self.seg(self.WITH_PROPERTIES_PREFIX, sep="")
            )
            return with_sql

        def property_sql(self, expression: exp.Property) -> str:
            property_cls = expression.__class__
            if property_cls == exp.Property:
                return f"{self.property_name(expression)}={self.sql(expression, 'value')}"

            property_name = exp.Properties.PROPERTY_TO_NAME.get(property_cls)

            if not property_name:
                self.unsupported(f"Unsupported property {expression.key}")

            if property_cls == exp.EncodeProperty:
                return self._encode_property_sql(expression)

            if property_cls == exp.FileFormatProperty:
                return f"FORMAT {self.sql(expression, 'this')}"

            if property_cls == exp.IncludeProperty:
                return self._include_property_sql(expression)

            return f"{property_name}={self.sql(expression, 'this')}"

        def create_sql(self, expression: exp.Create) -> str:
            kind = self.sql(expression, "kind")
            kind = self.dialect.INVERSE_CREATABLE_KIND_MAPPING.get(kind) or kind
            properties = expression.args.get("properties")
            properties_locs = self.locate_properties(properties) if properties else defaultdict()

            this = self.createable_sql(expression, properties_locs)

            properties_sql = ""
            if properties_locs.get(exp.Properties.Location.POST_SCHEMA) or properties_locs.get(
                exp.Properties.Location.POST_WITH
            ):
                properties_sql = self.sql(
                    exp.Properties(
                        expressions=[
                            *properties_locs[exp.Properties.Location.POST_SCHEMA],
                            *properties_locs[exp.Properties.Location.POST_WITH],
                        ]
                    )
                )

                if properties_locs.get(exp.Properties.Location.POST_SCHEMA):
                    properties_sql = self.sep() + properties_sql
                elif not self.pretty:
                    # Standalone POST_WITH properties need a leading whitespace in non-pretty mode
                    properties_sql = f" {properties_sql}"

            begin = " BEGIN" if expression.args.get("begin") else ""
            end = " END" if expression.args.get("end") else ""

            expression_sql = self.sql(expression, "expression")
            if expression_sql:
                expression_sql = f"{begin}{self.sep()}{expression_sql}{end}"

                if self.CREATE_FUNCTION_RETURN_AS or not isinstance(
                    expression.expression, exp.Return
                ):
                    postalias_props_sql = ""
                    if properties_locs.get(exp.Properties.Location.POST_ALIAS):
                        postalias_props_sql = self.properties(
                            exp.Properties(
                                expressions=properties_locs[exp.Properties.Location.POST_ALIAS]
                            ),
                            wrapped=False,
                        )
                    postalias_props_sql = f" {postalias_props_sql}" if postalias_props_sql else ""
                    expression_sql = f" AS{postalias_props_sql}{expression_sql}"

            postindex_props_sql = ""
            if properties_locs.get(exp.Properties.Location.POST_INDEX):
                postindex_props_sql = self.properties(
                    exp.Properties(expressions=properties_locs[exp.Properties.Location.POST_INDEX]),
                    wrapped=False,
                    prefix=" ",
                )

            indexes = self.expressions(expression, key="indexes", indent=False, sep=" ")
            indexes = f" {indexes}" if indexes else ""
            index_sql = indexes + postindex_props_sql

            replace = " OR REPLACE" if expression.args.get("replace") else ""
            refresh = " OR REFRESH" if expression.args.get("refresh") else ""
            unique = " UNIQUE" if expression.args.get("unique") else ""

            clustered = expression.args.get("clustered")
            if clustered is None:
                clustered_sql = ""
            elif clustered:
                clustered_sql = " CLUSTERED COLUMNSTORE"
            else:
                clustered_sql = " NONCLUSTERED COLUMNSTORE"

            postcreate_props_sql = ""
            if properties_locs.get(exp.Properties.Location.POST_CREATE):
                postcreate_props_sql = self.properties(
                    exp.Properties(
                        expressions=properties_locs[exp.Properties.Location.POST_CREATE]
                    ),
                    sep=" ",
                    prefix=" ",
                    wrapped=False,
                )

            modifiers = "".join((clustered_sql, replace, refresh, unique, postcreate_props_sql))

            postexpression_props_sql = ""
            if properties_locs.get(exp.Properties.Location.POST_EXPRESSION):
                postexpression_props_sql = self.properties(
                    exp.Properties(
                        expressions=properties_locs[exp.Properties.Location.POST_EXPRESSION]
                    ),
                    sep=" ",
                    prefix=" ",
                    wrapped=False,
                )

            if kind == "SINK" or kind == "SOURCE" and self.pretty:
                postexpression_props_sql = self.sep() + postexpression_props_sql.strip()

            concurrently = " CONCURRENTLY" if expression.args.get("concurrently") else ""
            exists_sql = " IF NOT EXISTS" if expression.args.get("exists") else ""
            no_schema_binding = (
                " WITH NO SCHEMA BINDING" if expression.args.get("no_schema_binding") else ""
            )

            clone = self.sql(expression, "clone")
            clone = f" {clone}" if clone else ""

            if kind == "SINK":
                # Tailored for risingwave sink.
                expression_sql = f"CREATE SINK{concurrently}{exists_sql} {this}{expression_sql}{properties_sql}{postexpression_props_sql}{index_sql}{no_schema_binding}{clone}"
            else:
                expression_sql = f"CREATE{modifiers} {kind}{concurrently}{exists_sql} {this}{properties_sql}{expression_sql}{postexpression_props_sql}{index_sql}{no_schema_binding}{clone}"
            return self.prepend_ctes(expression, expression_sql)

        def sql(
            self,
            expression: t.Optional[str | exp.Expression],
            key: t.Optional[str] = None,
            comment: bool = True,
        ) -> str:
            if not expression:
                return ""

            if isinstance(expression, str):
                return expression

            if key:
                value = expression.args.get(key)
                if value:
                    return self.sql(value)
                return ""

            transform = self.TRANSFORMS.get(expression.__class__)

            if callable(transform):
                sql = transform(self, expression)
            elif isinstance(expression, exp.Expression):
                exp_handler_name = f"{expression.key}_sql"
                if hasattr(self, exp_handler_name):
                    sql = getattr(self, exp_handler_name)(expression)
                elif isinstance(expression, exp.Func):
                    sql = self.function_fallback_sql(expression)
                elif isinstance(expression, exp.Property):
                    sql = self.property_sql(expression)
                elif isinstance(expression, exp.Watermark):
                    sql = self._watermark_sql(expression)
                else:
                    raise ValueError(f"Unsupported expression type {expression.__class__.__name__}")
            else:
                raise ValueError(
                    f"Expected an Expression. Received {type(expression)}: {expression}"
                )

            return self.maybe_comment(sql, expression) if self.comments and comment else sql
