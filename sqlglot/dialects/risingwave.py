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
        PROPERTY_PARSERS = {
            **Postgres.Parser.PROPERTY_PARSERS,
            "ENCODE": lambda self: self._parse_encode_property(is_key=False),
            "KEY": lambda self: self._parse_key_encode_property(),
            "INCLUDE": lambda self: self._parse_include_property(),
        }

        def _parse_table_hints(self) -> t.Optional[t.List[exp.Expression]]:
            # There is no hint in risingwave.
            # Do nothing here to avoid WITH keywords conflict in CREATE SINK statement.
            return None

        # @FIXME: this method look too complicated.
        def _parse_include_property(self) -> t.Optional[exp.Expression]:
            column_type = self._parse_var_or_string()
            column_alias = None
            inner_field = None
            header_inner_expect_type = None
            if not column_type:
                return None
            inner_field = self._parse_string() or self._parse_var()
            if inner_field:
                if self._match_set(self.TYPE_TOKENS, advance=False):
                    header_inner_expect_type = self._parse_var_or_string()

            include_property = self.expression(exp.IncludeProperty, column_type=column_type)

            if self._match(TokenType.ALIAS):
                column_alias = self._parse_var_or_string()
            if column_alias:
                include_property.set("column_alias", column_alias)
            if inner_field:
                include_property.set("inner_field", inner_field)
            if header_inner_expect_type:
                include_property.set("header_inner_expect_type", header_inner_expect_type)

            return include_property

        def _parse_key_encode_property(self) -> t.Optional[exp.Expression]:
            index = self._index - 1
            if not self._match_texts("ENCODE"):
                self._retreat(index)
                return None
            return self._parse_encode_property(is_key=True)

        def _parse_encode_property(self, is_key: bool) -> t.Optional[exp.Expression]:
            encode_var = self._parse_var_or_string()
            if not encode_var:
                return None
            encode_property: exp.Expression = self.expression(
                exp.EncodeProperty, this=encode_var, is_key=is_key
            )
            if self._match(TokenType.L_PAREN, advance=False):
                expressions = exp.Properties(expressions=self._parse_wrapped_properties())
                encode_property.set("expressions", expressions)
            return encode_property

        # @FIXME: refactor this method.
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
            # First parse if it is watermark field before parsing super class field def.
            return self._parse_watermark_field() or super()._parse_field_def()

        def _parse_column_def(self, this: t.Optional[exp.Expression]) -> t.Optional[exp.Expression]:
            # column defs are not really columns, they're identifiers
            if isinstance(this, exp.Column):
                this = this.this

            kind = self._parse_types(schema=True)
            constraints: t.List[exp.Expression] = []

            if (
                kind
                and self._match(TokenType.ALIAS, advance=False)
                and not self._match_pair(TokenType.ALIAS, TokenType.L_PAREN, advance=False)
            ):
                # Deal with CREATE SOURCE statement, where we may have col_name data_type [ AS generation_expression ] and generation_expression may not have parentheses.
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

                return self.expression(exp.ColumnDef, this=this, kind=kind, constraints=constraints)

            return super()._parse_column_def(this)

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

        def watermark_sql(self, expression: exp.Expression) -> str:
            column = expression.args.get("column")
            expression_str = expression.args.get("expression")
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

        def property_sql(self, expression: exp.Property) -> str:
            property_cls = expression.__class__
            if property_cls == exp.EncodeProperty:
                return self._encode_property_sql(expression)

            if property_cls == exp.FileFormatProperty:
                return f"FORMAT {self.sql(expression, 'this')}"

            if property_cls == exp.IncludeProperty:
                return self._include_property_sql(expression)

            return super().property_sql(expression)

        # @FIXME: refactor this method.
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
