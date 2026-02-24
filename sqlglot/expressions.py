"""
## Expressions

Every AST node in SQLGlot is represented by a subclass of `Expression`.

This module contains the implementation of all supported `Expression` types. Additionally,
it exposes a number of helper functions, which are mainly used to programmatically build
SQL expressions, such as `sqlglot.expressions.select`.

----
"""

from __future__ import annotations

import datetime
import logging
import math
import numbers
import re
import textwrap
import typing as t
from enum import auto
from functools import reduce

from sqlglot.errors import ErrorLevel, ParseError, TokenError
from sqlglot.expression_core import (  # noqa: F401
    Expression,
    Condition,
    Predicate,
    _Predicate,
    Binary,
    Unary,
    SubqueryPredicate,
    Add,
    Adjacent,
    All,
    Any,
    Between,
    BitwiseAnd,
    BitwiseLeftShift,
    BitwiseNot,
    BitwiseOr,
    BitwiseRightShift,
    BitwiseXor,
    Connector,
    Distance,
    Div,
    DPipe,
    Escape,
    ExtendsLeft,
    ExtendsRight,
    Glob,
    GT,
    GTE,
    ILike,
    In,
    IntDiv,
    Is,
    Kwarg,
    Like,
    LT,
    LTE,
    Match,
    Mod,
    Mul,
    NEQ,
    Not,
    NullSafeEQ,
    NullSafeNEQ,
    Operator,
    Overlaps,
    PropertyEQ,
    SimilarTo,
    Sub,
    AddConstraint,
    AddPartition,
    AlgorithmProperty,
    AlterColumn,
    AlterDistStyle,
    AlterIndex,
    AlterRename,
    AlterSession,
    AlterSet,
    AlterSortKey,
    AllowedValuesProperty,
    Analyze,
    AnalyzeDelete,
    AnalyzeHistogram,
    AnalyzeListChainedRows,
    AnalyzeSample,
    AnalyzeStatistics,
    AnalyzeValidate,
    AnalyzeWith,
    AtIndex,
    AtTimeZone,
    Attach,
    AttachOption,
    AutoIncrementColumnConstraint,
    AutoIncrementProperty,
    AutoRefreshProperty,
    BackupProperty,
    BitString,
    Block,
    BlockCompressionProperty,
    Boolean,
    BuildProperty,
    ByteString,
    Cache,
    CaseSpecificColumnConstraint,
    Changes,
    CharacterSet,
    CharacterSetColumnConstraint,
    CharacterSetProperty,
    Check,
    CheckColumnConstraint,
    ChecksumProperty,
    Clone,
    Cluster,
    ClusteredByProperty,
    ClusteredColumnConstraint,
    CollateColumnConstraint,
    CollateProperty,
    ColumnConstraint,
    ColumnConstraintKind,
    ColumnPosition,
    ColumnPrefix,
    Command,
    Comment,
    CommentColumnConstraint,
    Commit,
    Comprehension,
    CompressColumnConstraint,
    ComputedColumnConstraint,
    ConditionalInsert,
    Connect,
    Constraint,
    CopyGrantsProperty,
    CopyParameter,
    Credentials,
    CredentialsProperty,
    Cube,
    DataBlocksizeProperty,
    DataDeletionProperty,
    DataTypeParam,
    DateFormatColumnConstraint,
    Declare,
    DeclareItem,
    DefaultColumnConstraint,
    DefinerProperty,
    Describe,
    Detach,
    DictProperty,
    DictRange,
    DictSubProperty,
    Directory,
    DirectoryStage,
    Distinct,
    Distribute,
    DistKeyProperty,
    DistStyleProperty,
    DistributedByProperty,
    Drop,
    DropPartition,
    DuplicateKeyProperty,
    DynamicProperty,
    EmptyProperty,
    EncodeColumnConstraint,
    EncodeProperty,
    EndStatement,
    EngineProperty,
    EnviromentProperty,
    EphemeralColumnConstraint,
    EQ,
    ExcludeColumnConstraint,
    ExecuteAsProperty,
    Export,
    ExternalProperty,
    FallbackProperty,
    Fetch,
    FileFormatProperty,
    Filter,
    Final,
    ForIn,
    ForceProperty,
    ForeignKey,
    FormatPhrase,
    FreespaceProperty,
    From,
    FromTimeZone,
    GeneratedAsIdentityColumnConstraint,
    GeneratedAsRowColumnConstraint,
    Get,
    GlobalProperty,
    Grant,
    GrantPrincipal,
    GrantPrivilege,
    Group,
    GroupingSets,
    Having,
    HavingMax,
    HeapProperty,
    Heredoc,
    HexString,
    Hint,
    HistoricalData,
    IcebergProperty,
    Identifier,
    IfBlock,
    InOutColumnConstraint,
    IncludeProperty,
    Index,
    IndexColumnConstraint,
    IndexConstraintOption,
    IndexParameters,
    IndexTableHint,
    InheritsProperty,
    InlineLengthColumnConstraint,
    InputModelProperty,
    InputOutputFormat,
    Install,
    Into,
    Introducer,
    IsolatedLoadingProperty,
    JSON,
    JSONColumnDef,
    JSONExtractQuote,
    JSONKeyValue,
    JSONPathPart,
    JSONSchema,
    JSONValue,
    JoinHint,
    JournalProperty,
    Kill,
    Lambda,
    LanguageProperty,
    LikeProperty,
    Limit,
    LimitOptions,
    Literal,
    LoadData,
    LocationProperty,
    Lock,
    LockProperty,
    LockingProperty,
    LockingStatement,
    LogProperty,
    MaskingPolicyColumnConstraint,
    MatchRecognize,
    MatchRecognizeMeasure,
    MaterializedProperty,
    MergeBlockRatioProperty,
    MergeTreeTTL,
    MergeTreeTTLAction,
    ModelAttribute,
    MultitableInserts,
    National,
    NoPrimaryIndexProperty,
    NonClusteredColumnConstraint,
    NotForReplicationColumnConstraint,
    NotNullColumnConstraint,
    Null,
    Offset,
    OnCluster,
    OnCommitProperty,
    OnCondition,
    OnConflict,
    OnProperty,
    OnUpdateColumnConstraint,
    OpenJSONColumnDef,
    Opclass,
    Order,
    Ordered,
    OutputModelProperty,
    OverflowTruncateBehavior,
    Parameter,
    Partition,
    PartitionBoundSpec,
    PartitionByListProperty,
    PartitionByRangeProperty,
    PartitionByRangePropertyDynamic,
    PartitionByTruncate,
    PartitionId,
    PartitionList,
    PartitionRange,
    PartitionedByBucket,
    PartitionedByProperty,
    PartitionedOfProperty,
    PathColumnConstraint,
    PeriodForSystemTimeConstraint,
    PivotAny,
    Placeholder,
    Pragma,
    PrimaryKey,
    PrimaryKeyColumnConstraint,
    Prior,
    ProjectionDef,
    ProjectionPolicyColumnConstraint,
    Property,
    Put,
    Qualify,
    QueryBand,
    QueryOption,
    QueryTransform,
    RawString,
    RecursiveWithSearch,
    Reference,
    Refresh,
    RefreshTriggerProperty,
    RemoteWithConnectionModelProperty,
    RenameColumn,
    ReplacePartition,
    Return,
    Returning,
    ReturnsProperty,
    Revoke,
    Rollback,
    Rollup,
    RollupIndex,
    RollupProperty,
    RowFormatDelimitedProperty,
    RowFormatProperty,
    RowFormatSerdeProperty,
    SampleProperty,
    Schema,
    SchemaCommentProperty,
    ScopeResolution,
    SecureProperty,
    SecurityProperty,
    SemanticView,
    Semicolon,
    SequenceProperties,
    SerdeProperties,
    SessionParameter,
    Set,
    SetConfigProperty,
    SetItem,
    SetProperty,
    SettingsProperty,
    SharingProperty,
    Show,
    Slice,
    Sort,
    SortKeyProperty,
    SqlReadWriteProperty,
    SqlSecurityProperty,
    StabilityProperty,
    Star,
    StorageHandlerProperty,
    StoredProcedure,
    StreamingTableProperty,
    StrictProperty,
    Summarize,
    SwapTable,
    TableAlias,
    TableSample,
    Tag,
    TemporaryProperty,
    TitleColumnConstraint,
    ToTableProperty,
    Transaction,
    TransformModelProperty,
    TransientProperty,
    TranslateCharacters,
    TriggerEvent,
    TriggerExecute,
    TriggerProperties,
    TriggerReferencing,
    TruncateTable,
    Uncache,
    UnicodeString,
    UniqueColumnConstraint,
    UniqueKeyProperty,
    UnloggedProperty,
    UnpivotColumns,
    UppercaseColumnConstraint,
    Use,
    UserDefinedFunction,
    UsingTemplateProperty,
    Var,
    Version,
    ViewAttributeProperty,
    VolatileProperty,
    WatermarkColumnConstraint,
    When,
    Where,
    Whens,
    WhileBlock,
    Window,
    WindowSpec,
    With,
    WithDataProperty,
    WithFill,
    WithJournalTableProperty,
    WithOperator,
    WithProcedureOptions,
    WithSchemaBindingProperty,
    WithSystemVersioningProperty,
    WithTableHint,
    WithinGroup,
    XMLKeyValueOption,
    ZeroFillColumnConstraint,
    Alias,
    Aliases,
    Alter,
    AnalyzeColumns,
    Bracket,
    Column,
    COLUMN_PARTS,
    Dot,
    Execute,
    ExecuteSql,
    FormatJson,
    IgnoreNulls,
    JSONPath,
    JSONPathFilter,
    JSONPathKey,
    JSONPathRecursive,
    JSONPathRoot,
    JSONPathScript,
    JSONPathSlice,
    JSONPathSelector,
    JSONPathSubscript,
    JSONPathUnion,
    JSONPathWildcard,
    Neg,
    Paren,
    Pivot,
    PivotAlias,
    PositionalColumn,
    PreWhere,
    Pseudocolumn,
    RespectNulls,
    Stream,
    TableColumn,
    UsingData,
    Variadic,
    WeekStart,
    XMLNamespace,
    AIAgg,
    AIClassify,
    AISummarizeAgg,
    Abs,
    Acos,
    Acosh,
    AddMonths,
    AggFunc,
    Anonymous,
    AnonymousAggFunc,
    AnyValue,
    Apply,
    ApproxDistinct,
    ApproxPercentileAccumulate,
    ApproxPercentileCombine,
    ApproxPercentileEstimate,
    ApproxQuantile,
    ApproxQuantiles,
    ApproxTopK,
    ApproxTopKAccumulate,
    ApproxTopKCombine,
    ApproxTopKEstimate,
    ApproxTopSum,
    ApproximateSimilarity,
    ArgMax,
    ArgMin,
    Array,
    ArrayAgg,
    ArrayAll,
    ArrayAny,
    ArrayAppend,
    ArrayCompact,
    ArrayConcat,
    ArrayConcatAgg,
    ArrayConstructCompact,
    ArrayDistinct,
    ArrayExcept,
    ArrayFilter,
    ArrayFirst,
    ArrayInsert,
    ArrayIntersect,
    ArrayLast,
    ArrayMax,
    ArrayMin,
    ArrayPrepend,
    ArrayRemove,
    ArrayRemoveAt,
    ArrayReverse,
    ArraySize,
    ArraySlice,
    ArraySort,
    ArraySum,
    ArrayToString,
    ArrayUnionAgg,
    ArrayUniqueAgg,
    ArraysZip,
    Ascii,
    Asin,
    Asinh,
    Atan,
    Atan2,
    Atanh,
    Avg,
    Base64DecodeBinary,
    Base64DecodeString,
    Base64Encode,
    BitLength,
    BitmapBitPosition,
    BitmapBucketNumber,
    BitmapConstructAgg,
    BitmapCount,
    BitmapOrAgg,
    BitwiseAndAgg,
    BitwiseCount,
    BitwiseOrAgg,
    BitwiseXorAgg,
    Booland,
    Boolnot,
    Boolor,
    BoolxorAgg,
    ByteLength,
    Cast,
    CastToStrType,
    Cbrt,
    Ceil,
    CheckJson,
    CheckXml,
    Chr,
    Coalesce,
    CodePointsToBytes,
    CodePointsToString,
    Collation,
    Columns,
    CombinedAggFunc,
    CombinedParameterizedAgg,
    Compress,
    Concat,
    ConcatWs,
    ConnectByRoot,
    Contains,
    Convert,
    ConvertTimezone,
    ConvertToCharset,
    Cos,
    Cosh,
    CosineDistance,
    Cot,
    Coth,
    Count,
    CountIf,
    CovarPop,
    CovarSamp,
    Csc,
    Csch,
    CumeDist,
    CurrentAccount,
    CurrentAccountName,
    CurrentAvailableRoles,
    CurrentCatalog,
    CurrentClient,
    CurrentDatabase,
    CurrentDate,
    CurrentDatetime,
    CurrentIpAddress,
    CurrentOrganizationName,
    CurrentOrganizationUser,
    CurrentRegion,
    CurrentRole,
    CurrentRoleType,
    CurrentSchema,
    CurrentSchemas,
    CurrentSecondaryRoles,
    CurrentSession,
    CurrentStatement,
    CurrentTime,
    CurrentTimestamp,
    CurrentTimestampLTZ,
    CurrentTimezone,
    CurrentTransaction,
    CurrentUser,
    CurrentVersion,
    CurrentWarehouse,
    Date,
    DateFromParts,
    DateFromUnixDate,
    DateStrToDate,
    DateToDateStr,
    DateToDi,
    Datetime,
    Day,
    DayOfMonth,
    DayOfWeek,
    DayOfWeekIso,
    DayOfYear,
    Dayname,
    Decode,
    DecodeCase,
    DecompressBinary,
    DecompressString,
    Decrypt,
    DecryptRaw,
    Degrees,
    DenseRank,
    DiToDate,
    DotProduct,
    Elt,
    Encode,
    Encrypt,
    EncryptRaw,
    EndsWith,
    EqualNull,
    EuclideanDistance,
    Exp,
    ExplodingGenerateSeries,
    Extract,
    Factorial,
    FarmFingerprint,
    FeaturesAtTime,
    First,
    FirstValue,
    Flatten,
    Float64,
    Floor,
    Format,
    FromBase,
    FromBase32,
    FromBase64,
    FromISO8601Timestamp,
    Func,
    GapFill,
    GenerateDateArray,
    GenerateEmbedding,
    GenerateSeries,
    GenerateTimestampArray,
    GetExtract,
    Getbit,
    Greatest,
    GroupConcat,
    Grouping,
    GroupingId,
    HashAgg,
    Hex,
    HexDecodeString,
    HexEncode,
    Hll,
    Host,
    Hour,
    If,
    Initcap,
    Inline,
    Int64,
    IsArray,
    IsAscii,
    IsInf,
    IsNan,
    IsNullValue,
    JSONArray,
    JSONArrayAgg,
    JSONArrayAppend,
    JSONArrayInsert,
    JSONBExists,
    JSONBObjectAgg,
    JSONBool,
    JSONCast,
    JSONExists,
    JSONExtractArray,
    JSONFormat,
    JSONKeys,
    JSONKeysAtDepth,
    JSONObject,
    JSONObjectAgg,
    JSONRemove,
    JSONSet,
    JSONStripNulls,
    JSONTable,
    JSONType,
    JSONValueArray,
    JarowinklerSimilarity,
    JustifyDays,
    JustifyHours,
    JustifyInterval,
    Kurtosis,
    Lag,
    Last,
    LastValue,
    LaxBool,
    LaxFloat64,
    LaxInt64,
    LaxString,
    Lead,
    Least,
    Left,
    Length,
    Levenshtein,
    List,
    Ln,
    Localtime,
    Localtimestamp,
    Log,
    LogicalAnd,
    LogicalOr,
    Lower,
    LowerHex,
    MD5,
    MD5Digest,
    MD5NumberLower64,
    MD5NumberUpper64,
    MLForecast,
    MLTranslate,
    MakeInterval,
    ManhattanDistance,
    Map,
    MapCat,
    MapContainsKey,
    MapDelete,
    MapFromEntries,
    MapInsert,
    MapKeys,
    MapPick,
    MapSize,
    MatchAgainst,
    Max,
    Median,
    Min,
    Minhash,
    MinhashCombine,
    Minute,
    Mode,
    Month,
    Monthname,
    MonthsBetween,
    NetFunc,
    NextDay,
    NextValueFor,
    Normal,
    Normalize,
    NthValue,
    Ntile,
    Nullif,
    NumberToStr,
    Nvl2,
    ObjectAgg,
    ObjectId,
    ObjectInsert,
    OpenJSON,
    Overlay,
    Pad,
    ParameterizedAgg,
    ParseBignumeric,
    ParseDatetime,
    ParseIp,
    ParseJSON,
    ParseNumeric,
    ParseTime,
    ParseUrl,
    PercentRank,
    PercentileCont,
    PercentileDisc,
    Pi,
    Predict,
    PreviousDay,
    Quantile,
    Quarter,
    Radians,
    Rand,
    Randn,
    Randstr,
    RangeBucket,
    RangeN,
    Rank,
    ReadCSV,
    ReadParquet,
    Reduce,
    RegDomain,
    RegexpCount,
    RegexpExtract,
    RegexpExtractAll,
    RegexpInstr,
    RegexpReplace,
    RegexpSplit,
    RegrAvgx,
    RegrAvgy,
    RegrCount,
    RegrIntercept,
    RegrR2,
    RegrSlope,
    RegrSxx,
    RegrSxy,
    RegrSyy,
    RegrValx,
    RegrValy,
    Repeat,
    Replace,
    Reverse,
    Right,
    Round,
    RowNumber,
    RtrimmedLength,
    SHA,
    SHA1Digest,
    SHA2,
    SHA2Digest,
    SafeAdd,
    SafeConvertBytesToString,
    SafeDivide,
    SafeFunc,
    SafeMultiply,
    SafeNegate,
    SafeSubtract,
    Search,
    SearchIp,
    Sec,
    Sech,
    Second,
    Seq1,
    Seq2,
    Seq4,
    Seq8,
    SessionUser,
    Sign,
    Sin,
    Sinh,
    Skewness,
    SortArray,
    Soundex,
    SoundexP123,
    Space,
    Split,
    SplitPart,
    Sqrt,
    StDistance,
    StPoint,
    StandardHash,
    StarMap,
    StartsWith,
    Stddev,
    StddevPop,
    StddevSamp,
    Sum,
    StrPosition,
    StrToDate,
    StrToMap,
    StrToTime,
    StrToUnix,
    String,
    StringToArray,
    Struct,
    StructExtract,
    Stuff,
    Substring,
    SubstringIndex,
    Systimestamp,
    Tan,
    Tanh,
    Time,
    TimeFromParts,
    TimeStrToDate,
    TimeStrToTime,
    TimeStrToUnix,
    TimeToStr,
    TimeToTimeStr,
    TimeToUnix,
    Timestamp,
    TimestampFromParts,
    TimestampLtzFromParts,
    TimestampTzFromParts,
    ToArray,
    ToBase32,
    ToBase64,
    ToBinary,
    ToBoolean,
    ToChar,
    ToCodePoints,
    ToDays,
    ToDecfloat,
    ToDouble,
    ToFile,
    ToMap,
    ToNumber,
    Transform,
    Translate,
    Trim,
    Trunc,
    Try,
    TryBase64DecodeBinary,
    TryBase64DecodeString,
    TryCast,
    TryHexDecodeBinary,
    TryHexDecodeString,
    TryToDecfloat,
    TsOrDiToDi,
    TsOrDsToDate,
    TsOrDsToDateStr,
    TsOrDsToDatetime,
    TsOrDsToTime,
    TsOrDsToTimestamp,
    Typeof,
    Unhex,
    Unicode,
    Uniform,
    UnixDate,
    UnixMicros,
    UnixMillis,
    UnixSeconds,
    UnixToStr,
    UnixToTime,
    UnixToTimeStr,
    Upper,
    UtcDate,
    UtcTime,
    UtcTimestamp,
    Uuid,
    VarMap,
    Variance,
    VariancePop,
    VectorSearch,
    Week,
    WeekOfYear,
    WidthBucket,
    XMLElement,
    XMLGet,
    XMLTable,
    Year,
    YearOfWeek,
    YearOfWeekIso,
    Zipf,
)
from sqlglot.helper import (
    AutoName,
    ensure_collection,
    ensure_list,
    seq_get,
    split_num_words,
    subclasses,
)

from sqlglot.tokens import Token

if t.TYPE_CHECKING:
    from typing_extensions import Self

    from sqlglot._typing import E, Lit
    from sqlglot.dialects.dialect import DialectType

    Q = t.TypeVar("Q", bound="Query")
    S = t.TypeVar("S", bound="SetOperation")


logger = logging.getLogger("sqlglot")

SQLGLOT_ANONYMOUS = "sqlglot.anonymous"
TABLE_PARTS = ("this", "db", "catalog")


IntoType = t.Union[
    str,
    t.Type[Expression],
    t.Collection[t.Union[str, t.Type[Expression]]],
]
ExpOrStr = t.Union[str, Expression]


class DerivedTable(Expression):
    @property
    def selects(self) -> t.List[Expression]:
        return self.this.selects if isinstance(self.this, Query) else []

    @property
    def named_selects(self) -> t.List[str]:
        return [select.output_name for select in self.selects]


class Query(Expression):
    def subquery(self, alias: t.Optional[ExpOrStr] = None, copy: bool = True) -> Subquery:
        """
        Returns a `Subquery` that wraps around this query.

        Example:
            >>> subquery = Select().select("x").from_("tbl").subquery()
            >>> Select().select("x").from_(subquery).sql()
            'SELECT x FROM (SELECT x FROM tbl)'

        Args:
            alias: an optional alias for the subquery.
            copy: if `False`, modify this expression instance in-place.
        """
        instance = maybe_copy(self, copy)
        if not isinstance(alias, Expression):
            alias = TableAlias(this=to_identifier(alias)) if alias else None

        return Subquery(this=instance, alias=alias)

    def limit(
        self: Q, expression: ExpOrStr | int, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Q:
        """
        Adds a LIMIT clause to this query.

        Example:
            >>> select("1").union(select("1")).limit(1).sql()
            'SELECT 1 UNION SELECT 1 LIMIT 1'

        Args:
            expression: the SQL code string to parse.
                This can also be an integer.
                If a `Limit` instance is passed, it will be used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Limit`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            A limited Select expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="limit",
            into=Limit,
            prefix="LIMIT",
            dialect=dialect,
            copy=copy,
            into_arg="expression",
            **opts,
        )

    def offset(
        self: Q, expression: ExpOrStr | int, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Q:
        """
        Set the OFFSET expression.

        Example:
            >>> Select().from_("tbl").select("x").offset(10).sql()
            'SELECT x FROM tbl OFFSET 10'

        Args:
            expression: the SQL code string to parse.
                This can also be an integer.
                If a `Offset` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Offset`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="offset",
            into=Offset,
            prefix="OFFSET",
            dialect=dialect,
            copy=copy,
            into_arg="expression",
            **opts,
        )

    def order_by(
        self: Q,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Q:
        """
        Set the ORDER BY expression.

        Example:
            >>> Select().from_("tbl").select("x").order_by("x DESC").sql()
            'SELECT x FROM tbl ORDER BY x DESC'

        Args:
            *expressions: the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Order`.
            append: if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="order",
            append=append,
            copy=copy,
            prefix="ORDER BY",
            into=Order,
            dialect=dialect,
            **opts,
        )

    @property
    def ctes(self) -> t.List[CTE]:
        """Returns a list of all the CTEs attached to this query."""
        with_ = self.args.get("with_")
        return with_.expressions if with_ else []

    @property
    def selects(self) -> t.List[Expression]:
        """Returns the query's projections."""
        raise NotImplementedError("Query objects must implement `selects`")

    @property
    def named_selects(self) -> t.List[str]:
        """Returns the output names of the query's projections."""
        raise NotImplementedError("Query objects must implement `named_selects`")

    def select(
        self: Q,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Q:
        """
        Append to or set the SELECT expressions.

        Example:
            >>> Select().select("x", "y").sql()
            'SELECT x, y'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Query expression.
        """
        raise NotImplementedError("Query objects must implement `select`")

    def where(
        self: Q,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Q:
        """
        Append to or set the WHERE expressions.

        Examples:
            >>> Select().select("x").from_("tbl").where("x = 'a' OR x < 'b'").sql()
            "SELECT x FROM tbl WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_conjunction_builder(
            *[expr.this if isinstance(expr, Where) else expr for expr in expressions],
            instance=self,
            arg="where",
            append=append,
            into=Where,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def with_(
        self: Q,
        alias: ExpOrStr,
        as_: ExpOrStr,
        recursive: t.Optional[bool] = None,
        materialized: t.Optional[bool] = None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        scalar: t.Optional[bool] = None,
        **opts,
    ) -> Q:
        """
        Append to or set the common table expressions.

        Example:
            >>> Select().with_("tbl2", as_="SELECT * FROM tbl").select("x").from_("tbl2").sql()
            'WITH tbl2 AS (SELECT * FROM tbl) SELECT x FROM tbl2'

        Args:
            alias: the SQL code string to parse as the table name.
                If an `Expression` instance is passed, this is used as-is.
            as_: the SQL code string to parse as the table expression.
                If an `Expression` instance is passed, it will be used as-is.
            recursive: set the RECURSIVE part of the expression. Defaults to `False`.
            materialized: set the MATERIALIZED part of the expression.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            scalar: if `True`, this is a scalar common table expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_cte_builder(
            self,
            alias,
            as_,
            recursive=recursive,
            materialized=materialized,
            append=append,
            dialect=dialect,
            copy=copy,
            scalar=scalar,
            **opts,
        )

    def union(
        self, *expressions: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
    ) -> Union:
        """
        Builds a UNION expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").union("SELECT * FROM bla").sql()
            'SELECT * FROM foo UNION SELECT * FROM bla'

        Args:
            expressions: the SQL code strings.
                If `Expression` instances are passed, they will be used as-is.
            distinct: set the DISTINCT flag if and only if this is true.
            dialect: the dialect used to parse the input expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Union expression.
        """
        return union(self, *expressions, distinct=distinct, dialect=dialect, **opts)

    def intersect(
        self, *expressions: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
    ) -> Intersect:
        """
        Builds an INTERSECT expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").intersect("SELECT * FROM bla").sql()
            'SELECT * FROM foo INTERSECT SELECT * FROM bla'

        Args:
            expressions: the SQL code strings.
                If `Expression` instances are passed, they will be used as-is.
            distinct: set the DISTINCT flag if and only if this is true.
            dialect: the dialect used to parse the input expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Intersect expression.
        """
        return intersect(self, *expressions, distinct=distinct, dialect=dialect, **opts)

    def except_(
        self, *expressions: ExpOrStr, distinct: bool = True, dialect: DialectType = None, **opts
    ) -> Except:
        """
        Builds an EXCEPT expression.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("SELECT * FROM foo").except_("SELECT * FROM bla").sql()
            'SELECT * FROM foo EXCEPT SELECT * FROM bla'

        Args:
            expressions: the SQL code strings.
                If `Expression` instance are passed, they will be used as-is.
            distinct: set the DISTINCT flag if and only if this is true.
            dialect: the dialect used to parse the input expression.
            opts: other options to use to parse the input expressions.

        Returns:
            The new Except expression.
        """
        return except_(self, *expressions, distinct=distinct, dialect=dialect, **opts)


class UDTF(DerivedTable):
    @property
    def selects(self) -> t.List[Expression]:
        alias = self.args.get("alias")
        return alias.columns if alias else []


class DDL(Expression):
    @property
    def ctes(self) -> t.List[CTE]:
        """Returns a list of all the CTEs attached to this statement."""
        with_ = self.args.get("with_")
        return with_.expressions if with_ else []

    @property
    def selects(self) -> t.List[Expression]:
        """If this statement contains a query (e.g. a CTAS), this returns the query's projections."""
        return self.expression.selects if isinstance(self.expression, Query) else []

    @property
    def named_selects(self) -> t.List[str]:
        """
        If this statement contains a query (e.g. a CTAS), this returns the output
        names of the query's projections.
        """
        return self.expression.named_selects if isinstance(self.expression, Query) else []


class DML(Expression):
    def returning(
        self,
        expression: ExpOrStr,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> "Self":
        """
        Set the RETURNING expression. Not supported by all dialects.

        Example:
            >>> delete("tbl").returning("*", dialect="postgres").sql()
            'DELETE FROM tbl RETURNING *'

        Args:
            expression: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Delete: the modified expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="returning",
            prefix="RETURNING",
            dialect=dialect,
            copy=copy,
            into=Returning,
            **opts,
        )


class Create(DDL):
    arg_types = {
        "with_": False,
        "this": True,
        "kind": True,
        "expression": False,
        "exists": False,
        "properties": False,
        "replace": False,
        "refresh": False,
        "unique": False,
        "indexes": False,
        "no_schema_binding": False,
        "begin": False,
        "clone": False,
        "concurrently": False,
        "clustered": False,
    }

    @property
    def kind(self) -> t.Optional[str]:
        kind = self.args.get("kind")
        return kind and kind.upper()


# clickhouse supports scalar ctes
# https://clickhouse.com/docs/en/sql-reference/statements/select/with
class CTE(DerivedTable):
    arg_types = {
        "this": True,
        "alias": True,
        "scalar": False,
        "materialized": False,
        "key_expressions": False,
    }


class ColumnDef(Expression):
    arg_types = {
        "this": True,
        "kind": False,
        "constraints": False,
        "exists": False,
        "position": False,
        "default": False,
        "output": False,
    }

    @property
    def constraints(self) -> t.List[ColumnConstraint]:
        return self.args.get("constraints") or []

    @property
    def kind(self) -> t.Optional[DataType]:
        return self.args.get("kind")


class Delete(DML):
    arg_types = {
        "with_": False,
        "this": False,
        "using": False,
        "where": False,
        "returning": False,
        "order": False,
        "limit": False,
        "tables": False,  # Multiple-Table Syntax (MySQL)
        "cluster": False,  # Clickhouse
    }

    def delete(
        self,
        table: ExpOrStr,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Delete:
        """
        Create a DELETE expression or replace the table on an existing DELETE expression.

        Example:
            >>> delete("tbl").sql()
            'DELETE FROM tbl'

        Args:
            table: the table from which to delete.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Delete: the modified expression.
        """
        return _apply_builder(
            expression=table,
            instance=self,
            arg="this",
            dialect=dialect,
            into=Table,
            copy=copy,
            **opts,
        )

    def where(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Delete:
        """
        Append to or set the WHERE expressions.

        Example:
            >>> delete("tbl").where("x = 'a' OR x < 'b'").sql()
            "DELETE FROM tbl WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Delete: the modified expression.
        """
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="where",
            append=append,
            into=Where,
            dialect=dialect,
            copy=copy,
            **opts,
        )


class Copy(DML):
    arg_types = {
        "this": True,
        "kind": True,
        "files": False,
        "credentials": False,
        "format": False,
        "params": False,
    }


class Insert(DDL, DML):
    arg_types = {
        "hint": False,
        "with_": False,
        "is_function": False,
        "this": False,
        "expression": False,
        "conflict": False,
        "returning": False,
        "overwrite": False,
        "exists": False,
        "alternative": False,
        "where": False,
        "ignore": False,
        "by_name": False,
        "stored": False,
        "partition": False,
        "settings": False,
        "source": False,
        "default": False,
    }

    def with_(
        self,
        alias: ExpOrStr,
        as_: ExpOrStr,
        recursive: t.Optional[bool] = None,
        materialized: t.Optional[bool] = None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Insert:
        """
        Append to or set the common table expressions.

        Example:
            >>> insert("SELECT x FROM cte", "t").with_("cte", as_="SELECT * FROM tbl").sql()
            'WITH cte AS (SELECT * FROM tbl) INSERT INTO t SELECT x FROM cte'

        Args:
            alias: the SQL code string to parse as the table name.
                If an `Expression` instance is passed, this is used as-is.
            as_: the SQL code string to parse as the table expression.
                If an `Expression` instance is passed, it will be used as-is.
            recursive: set the RECURSIVE part of the expression. Defaults to `False`.
            materialized: set the MATERIALIZED part of the expression.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_cte_builder(
            self,
            alias,
            as_,
            recursive=recursive,
            materialized=materialized,
            append=append,
            dialect=dialect,
            copy=copy,
            **opts,
        )


class Join(Expression):
    arg_types = {
        "this": True,
        "on": False,
        "side": False,
        "kind": False,
        "using": False,
        "method": False,
        "global_": False,
        "hint": False,
        "match_condition": False,  # Snowflake
        "directed": False,  # Snowflake
        "expressions": False,
        "pivots": False,
    }

    @property
    def method(self) -> str:
        return self.text("method").upper()

    @property
    def kind(self) -> str:
        return self.text("kind").upper()

    @property
    def side(self) -> str:
        return self.text("side").upper()

    @property
    def hint(self) -> str:
        return self.text("hint").upper()

    @property
    def alias_or_name(self) -> str:
        return self.this.alias_or_name

    @property
    def is_semi_or_anti_join(self) -> bool:
        return self.kind in ("SEMI", "ANTI")

    def on(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Join:
        """
        Append to or set the ON expressions.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("JOIN x", into=Join).on("y = 1").sql()
            'JOIN x ON y = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Join expression.
        """
        join = _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="on",
            append=append,
            dialect=dialect,
            copy=copy,
            **opts,
        )

        if join.kind == "CROSS":
            join.set("kind", None)

        return join

    def using(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Join:
        """
        Append to or set the USING expressions.

        Example:
            >>> import sqlglot
            >>> sqlglot.parse_one("JOIN x", into=Join).using("foo", "bla").sql()
            'JOIN x USING (foo, bla)'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append: if `True`, concatenate the new expressions to the existing "using" list.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Join expression.
        """
        join = _apply_list_builder(
            *expressions,
            instance=self,
            arg="using",
            append=append,
            dialect=dialect,
            copy=copy,
            **opts,
        )

        if join.kind == "CROSS":
            join.set("kind", None)

        return join


class Lateral(UDTF):
    arg_types = {
        "this": True,
        "view": False,
        "outer": False,
        "alias": False,
        "cross_apply": False,  # True -> CROSS APPLY, False -> OUTER APPLY
        "ordinality": False,
    }


# https://docs.snowflake.com/sql-reference/literals-table
# https://docs.snowflake.com/en/sql-reference/functions-table#using-a-table-function
class TableFromRows(UDTF):
    arg_types = {
        "this": True,
        "alias": False,
        "joins": False,
        "pivots": False,
        "sample": False,
    }


class Tags(ColumnConstraintKind, Property):
    arg_types = {"expressions": True}


class Properties(Expression):
    arg_types = {"expressions": True}

    NAME_TO_PROPERTY = {
        "ALGORITHM": AlgorithmProperty,
        "AUTO_INCREMENT": AutoIncrementProperty,
        "CHARACTER SET": CharacterSetProperty,
        "CLUSTERED_BY": ClusteredByProperty,
        "COLLATE": CollateProperty,
        "COMMENT": SchemaCommentProperty,
        "CREDENTIALS": CredentialsProperty,
        "DEFINER": DefinerProperty,
        "DISTKEY": DistKeyProperty,
        "DISTRIBUTED_BY": DistributedByProperty,
        "DISTSTYLE": DistStyleProperty,
        "ENGINE": EngineProperty,
        "EXECUTE AS": ExecuteAsProperty,
        "FORMAT": FileFormatProperty,
        "LANGUAGE": LanguageProperty,
        "LOCATION": LocationProperty,
        "LOCK": LockProperty,
        "PARTITIONED_BY": PartitionedByProperty,
        "RETURNS": ReturnsProperty,
        "ROW_FORMAT": RowFormatProperty,
        "SORTKEY": SortKeyProperty,
        "ENCODE": EncodeProperty,
        "INCLUDE": IncludeProperty,
    }

    PROPERTY_TO_NAME = {v: k for k, v in NAME_TO_PROPERTY.items()}

    # CREATE property locations
    # Form: schema specified
    #   create [POST_CREATE]
    #     table a [POST_NAME]
    #     (b int) [POST_SCHEMA]
    #     with ([POST_WITH])
    #     index (b) [POST_INDEX]
    #
    # Form: alias selection
    #   create [POST_CREATE]
    #     table a [POST_NAME]
    #     as [POST_ALIAS] (select * from b) [POST_EXPRESSION]
    #     index (c) [POST_INDEX]
    class Location(AutoName):
        POST_CREATE = auto()
        POST_NAME = auto()
        POST_SCHEMA = auto()
        POST_WITH = auto()
        POST_ALIAS = auto()
        POST_EXPRESSION = auto()
        POST_INDEX = auto()
        UNSUPPORTED = auto()

    @classmethod
    def from_dict(cls, properties_dict: t.Dict) -> Properties:
        expressions = []
        for key, value in properties_dict.items():
            property_cls = cls.NAME_TO_PROPERTY.get(key.upper())
            if property_cls:
                expressions.append(property_cls(this=convert(value)))
            else:
                expressions.append(Property(this=Literal.string(key), value=convert(value)))

        return cls(expressions=expressions)


class Tuple(Expression):
    arg_types = {"expressions": False}

    def isin(
        self,
        *expressions: t.Any,
        query: t.Optional[ExpOrStr] = None,
        unnest: t.Optional[ExpOrStr] | t.Collection[ExpOrStr] = None,
        copy: bool = True,
        **opts,
    ) -> In:
        return In(
            this=maybe_copy(self, copy),
            expressions=[convert(e, copy=copy) for e in expressions],
            query=maybe_parse(query, copy=copy, **opts) if query else None,
            unnest=(
                Unnest(
                    expressions=[
                        maybe_parse(t.cast(ExpOrStr, e), copy=copy, **opts)
                        for e in ensure_list(unnest)
                    ]
                )
                if unnest
                else None
            ),
        )


QUERY_MODIFIERS = {
    "match": False,
    "laterals": False,
    "joins": False,
    "connect": False,
    "pivots": False,
    "prewhere": False,
    "where": False,
    "group": False,
    "having": False,
    "qualify": False,
    "windows": False,
    "distribute": False,
    "sort": False,
    "cluster": False,
    "order": False,
    "limit": False,
    "offset": False,
    "locks": False,
    "sample": False,
    "settings": False,
    "format": False,
    "options": False,
}


class Table(Expression):
    arg_types = {
        "this": False,
        "alias": False,
        "db": False,
        "catalog": False,
        "laterals": False,
        "joins": False,
        "pivots": False,
        "hints": False,
        "system_time": False,
        "version": False,
        "format": False,
        "pattern": False,
        "ordinality": False,
        "when": False,
        "only": False,
        "partition": False,
        "changes": False,
        "rows_from": False,
        "sample": False,
        "indexed": False,
    }

    @property
    def name(self) -> str:
        if not self.this or isinstance(self.this, Func):
            return ""
        return self.this.name

    @property
    def db(self) -> str:
        return self.text("db")

    @property
    def catalog(self) -> str:
        return self.text("catalog")

    @property
    def selects(self) -> t.List[Expression]:
        return []

    @property
    def named_selects(self) -> t.List[str]:
        return []

    @property
    def parts(self) -> t.List[Expression]:
        """Return the parts of a table in order catalog, db, table."""
        parts: t.List[Expression] = []

        for arg in ("catalog", "db", "this"):
            part = self.args.get(arg)

            if isinstance(part, Dot):
                parts.extend(part.flatten())
            elif isinstance(part, Expression):
                parts.append(part)

        return parts

    def to_column(self, copy: bool = True) -> Expression:
        parts = self.parts
        last_part = parts[-1]

        if isinstance(last_part, Identifier):
            col: Expression = column(*reversed(parts[0:4]), fields=parts[4:], copy=copy)  # type: ignore
        else:
            # This branch will be reached if a function or array is wrapped in a `Table`
            col = last_part

        alias = self.args.get("alias")
        if alias:
            col = alias_(col, alias.this, copy=copy)

        return col


class SetOperation(Query):
    arg_types = {
        "with_": False,
        "this": True,
        "expression": True,
        "distinct": False,
        "by_name": False,
        "side": False,
        "kind": False,
        "on": False,
        **QUERY_MODIFIERS,
    }

    def select(
        self: S,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> S:
        this = maybe_copy(self, copy)
        this.this.unnest().select(*expressions, append=append, dialect=dialect, copy=False, **opts)
        this.expression.unnest().select(
            *expressions, append=append, dialect=dialect, copy=False, **opts
        )
        return this

    @property
    def named_selects(self) -> t.List[str]:
        expression = self
        while isinstance(expression, SetOperation):
            expression = expression.this.unnest()
        return expression.named_selects

    @property
    def is_star(self) -> bool:
        return self.this.is_star or self.expression.is_star

    @property
    def selects(self) -> t.List[Expression]:
        expression = self
        while isinstance(expression, SetOperation):
            expression = expression.this.unnest()
        return expression.selects

    @property
    def left(self) -> Query:
        return self.this

    @property
    def right(self) -> Query:
        return self.expression

    @property
    def kind(self) -> str:
        return self.text("kind").upper()

    @property
    def side(self) -> str:
        return self.text("side").upper()


class Union(SetOperation):
    pass


class Except(SetOperation):
    pass


class Intersect(SetOperation):
    pass


class Update(DML):
    arg_types = {
        "with_": False,
        "this": False,
        "expressions": False,
        "from_": False,
        "where": False,
        "returning": False,
        "order": False,
        "limit": False,
        "options": False,
    }

    def table(
        self, expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Update:
        """
        Set the table to update.

        Example:
            >>> Update().table("my_table").set_("x = 1").sql()
            'UPDATE my_table SET x = 1'

        Args:
            expression : the SQL code strings to parse.
                If a `Table` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Table`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Update expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="this",
            into=Table,
            prefix=None,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def set_(
        self,
        *expressions: ExpOrStr,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Update:
        """
        Append to or set the SET expressions.

        Example:
            >>> Update().table("my_table").set_("x = 1").sql()
            'UPDATE my_table SET x = 1'

        Args:
            *expressions: the SQL code strings to parse.
                If `Expression` instance(s) are passed, they will be used as-is.
                Multiple expressions are combined with a comma.
            append: if `True`, add the new expressions to any existing SET expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.
        """
        return _apply_list_builder(
            *expressions,
            instance=self,
            arg="expressions",
            append=append,
            into=Expression,
            prefix=None,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def where(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Append to or set the WHERE expressions.

        Example:
            >>> Update().table("tbl").set_("x = 1").where("x = 'a' OR x < 'b'").sql()
            "UPDATE tbl SET x = 1 WHERE x = 'a' OR x < 'b'"

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
        """
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="where",
            append=append,
            into=Where,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def from_(
        self,
        expression: t.Optional[ExpOrStr] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Update:
        """
        Set the FROM expression.

        Example:
            >>> Update().table("my_table").set_("x = 1").from_("baz").sql()
            'UPDATE my_table SET x = 1 FROM baz'

        Args:
            expression : the SQL code strings to parse.
                If a `From` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `From`.
                If nothing is passed in then a from is not applied to the expression
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Update expression.
        """
        if not expression:
            return maybe_copy(self, copy)

        return _apply_builder(
            expression=expression,
            instance=self,
            arg="from_",
            into=From,
            prefix="FROM",
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def with_(
        self,
        alias: ExpOrStr,
        as_: ExpOrStr,
        recursive: t.Optional[bool] = None,
        materialized: t.Optional[bool] = None,
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Update:
        """
        Append to or set the common table expressions.

        Example:
            >>> Update().table("my_table").set_("x = 1").from_("baz").with_("baz", "SELECT id FROM foo").sql()
            'WITH baz AS (SELECT id FROM foo) UPDATE my_table SET x = 1 FROM baz'

        Args:
            alias: the SQL code string to parse as the table name.
                If an `Expression` instance is passed, this is used as-is.
            as_: the SQL code string to parse as the table expression.
                If an `Expression` instance is passed, it will be used as-is.
            recursive: set the RECURSIVE part of the expression. Defaults to `False`.
            materialized: set the MATERIALIZED part of the expression.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified expression.
        """
        return _apply_cte_builder(
            self,
            alias,
            as_,
            recursive=recursive,
            materialized=materialized,
            append=append,
            dialect=dialect,
            copy=copy,
            **opts,
        )


# DuckDB supports VALUES followed by https://duckdb.org/docs/stable/sql/query_syntax/limit
class Values(UDTF):
    arg_types = {
        "expressions": True,
        "alias": False,
        "order": False,
        "limit": False,
        "offset": False,
    }


# In Redshift, * and EXCLUDE can be separated with column projections (e.g., SELECT *, col1 EXCLUDE (col2))
# The "exclude" arg enables correct parsing and transpilation of this clause
class Select(Query):
    arg_types = {
        "with_": False,
        "kind": False,
        "expressions": False,
        "hint": False,
        "distinct": False,
        "into": False,
        "from_": False,
        "operation_modifiers": False,
        "exclude": False,
        **QUERY_MODIFIERS,
    }

    def from_(
        self, expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts
    ) -> Select:
        """
        Set the FROM expression.

        Example:
            >>> Select().from_("tbl").select("x").sql()
            'SELECT x FROM tbl'

        Args:
            expression : the SQL code strings to parse.
                If a `From` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `From`.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_builder(
            expression=expression,
            instance=self,
            arg="from_",
            into=From,
            prefix="FROM",
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def group_by(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Set the GROUP BY expression.

        Example:
            >>> Select().from_("tbl").select("x", "COUNT(1)").group_by("x").sql()
            'SELECT x, COUNT(1) FROM tbl GROUP BY x'

        Args:
            *expressions: the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Group`.
                If nothing is passed in then a group by is not applied to the expression
            append: if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Group` expression into a single expression.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        if not expressions:
            return self if not copy else self.copy()

        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="group",
            append=append,
            copy=copy,
            prefix="GROUP BY",
            into=Group,
            dialect=dialect,
            **opts,
        )

    def sort_by(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Set the SORT BY expression.

        Example:
            >>> Select().from_("tbl").select("x").sort_by("x DESC").sql(dialect="hive")
            'SELECT x FROM tbl SORT BY x DESC'

        Args:
            *expressions: the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `SORT`.
            append: if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="sort",
            append=append,
            copy=copy,
            prefix="SORT BY",
            into=Sort,
            dialect=dialect,
            **opts,
        )

    def cluster_by(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Set the CLUSTER BY expression.

        Example:
            >>> Select().from_("tbl").select("x").cluster_by("x DESC").sql(dialect="hive")
            'SELECT x FROM tbl CLUSTER BY x DESC'

        Args:
            *expressions: the SQL code strings to parse.
                If a `Group` instance is passed, this is used as-is.
                If another `Expression` instance is passed, it will be wrapped in a `Cluster`.
            append: if `True`, add to any existing expressions.
                Otherwise, this flattens all the `Order` expression into a single expression.
            dialect: the dialect used to parse the input expression.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_child_list_builder(
            *expressions,
            instance=self,
            arg="cluster",
            append=append,
            copy=copy,
            prefix="CLUSTER BY",
            into=Cluster,
            dialect=dialect,
            **opts,
        )

    def select(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        return _apply_list_builder(
            *expressions,
            instance=self,
            arg="expressions",
            append=append,
            dialect=dialect,
            into=Expression,
            copy=copy,
            **opts,
        )

    def lateral(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Append to or set the LATERAL expressions.

        Example:
            >>> Select().select("x").lateral("OUTER explode(y) tbl2 AS z").from_("tbl").sql()
            'SELECT x FROM tbl LATERAL VIEW OUTER EXPLODE(y) tbl2 AS z'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_list_builder(
            *expressions,
            instance=self,
            arg="laterals",
            append=append,
            into=Lateral,
            prefix="LATERAL VIEW",
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def join(
        self,
        expression: ExpOrStr,
        on: t.Optional[ExpOrStr] = None,
        using: t.Optional[ExpOrStr | t.Collection[ExpOrStr]] = None,
        append: bool = True,
        join_type: t.Optional[str] = None,
        join_alias: t.Optional[Identifier | str] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Append to or set the JOIN expressions.

        Example:
            >>> Select().select("*").from_("tbl").join("tbl2", on="tbl1.y = tbl2.y").sql()
            'SELECT * FROM tbl JOIN tbl2 ON tbl1.y = tbl2.y'

            >>> Select().select("1").from_("a").join("b", using=["x", "y", "z"]).sql()
            'SELECT 1 FROM a JOIN b USING (x, y, z)'

            Use `join_type` to change the type of join:

            >>> Select().select("*").from_("tbl").join("tbl2", on="tbl1.y = tbl2.y", join_type="left outer").sql()
            'SELECT * FROM tbl LEFT OUTER JOIN tbl2 ON tbl1.y = tbl2.y'

        Args:
            expression: the SQL code string to parse.
                If an `Expression` instance is passed, it will be used as-is.
            on: optionally specify the join "on" criteria as a SQL string.
                If an `Expression` instance is passed, it will be used as-is.
            using: optionally specify the join "using" criteria as a SQL string.
                If an `Expression` instance is passed, it will be used as-is.
            append: if `True`, add to any existing expressions.
                Otherwise, this resets the expressions.
            join_type: if set, alter the parsed join type.
            join_alias: an optional alias for the joined source.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            Select: the modified expression.
        """
        parse_args: t.Dict[str, t.Any] = {"dialect": dialect, **opts}

        try:
            expression = maybe_parse(expression, into=Join, prefix="JOIN", **parse_args)
        except ParseError:
            expression = maybe_parse(expression, into=(Join, Expression), **parse_args)

        join = expression if isinstance(expression, Join) else Join(this=expression)

        if isinstance(join.this, Select):
            join.this.replace(join.this.subquery())

        if join_type:
            method: t.Optional[Token]
            side: t.Optional[Token]
            kind: t.Optional[Token]

            method, side, kind = maybe_parse(join_type, into="JOIN_TYPE", **parse_args)  # type: ignore

            if method:
                join.set("method", method.text)
            if side:
                join.set("side", side.text)
            if kind:
                join.set("kind", kind.text)

        if on:
            on = and_(*ensure_list(on), dialect=dialect, copy=copy, **opts)
            join.set("on", on)

        if using:
            join = _apply_list_builder(
                *ensure_list(using),
                instance=join,
                arg="using",
                append=append,
                copy=copy,
                into=Identifier,
                **opts,
            )

        if join_alias:
            join.set("this", alias_(join.this, join_alias, table=True))

        return _apply_list_builder(
            join,
            instance=self,
            arg="joins",
            append=append,
            copy=copy,
            **opts,
        )

    def having(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        """
        Append to or set the HAVING expressions.

        Example:
            >>> Select().select("x", "COUNT(y)").from_("tbl").group_by("x").having("COUNT(y) > 3").sql()
            'SELECT x, COUNT(y) FROM tbl GROUP BY x HAVING COUNT(y) > 3'

        Args:
            *expressions: the SQL code strings to parse.
                If an `Expression` instance is passed, it will be used as-is.
                Multiple expressions are combined with an AND operator.
            append: if `True`, AND the new expressions to any existing expression.
                Otherwise, this resets the expression.
            dialect: the dialect used to parse the input expressions.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input expressions.

        Returns:
            The modified Select expression.
        """
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="having",
            append=append,
            into=Having,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def window(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        return _apply_list_builder(
            *expressions,
            instance=self,
            arg="windows",
            append=append,
            into=Window,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def qualify(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Select:
        return _apply_conjunction_builder(
            *expressions,
            instance=self,
            arg="qualify",
            append=append,
            into=Qualify,
            dialect=dialect,
            copy=copy,
            **opts,
        )

    def distinct(
        self, *ons: t.Optional[ExpOrStr], distinct: bool = True, copy: bool = True
    ) -> Select:
        """
        Set the OFFSET expression.

        Example:
            >>> Select().from_("tbl").select("x").distinct().sql()
            'SELECT DISTINCT x FROM tbl'

        Args:
            ons: the expressions to distinct on
            distinct: whether the Select should be distinct
            copy: if `False`, modify this expression instance in-place.

        Returns:
            Select: the modified expression.
        """
        instance = maybe_copy(self, copy)
        on = Tuple(expressions=[maybe_parse(on, copy=copy) for on in ons if on]) if ons else None
        instance.set("distinct", Distinct(on=on) if distinct else None)
        return instance

    def ctas(
        self,
        table: ExpOrStr,
        properties: t.Optional[t.Dict] = None,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Create:
        """
        Convert this expression to a CREATE TABLE AS statement.

        Example:
            >>> Select().select("*").from_("tbl").ctas("x").sql()
            'CREATE TABLE x AS SELECT * FROM tbl'

        Args:
            table: the SQL code string to parse as the table name.
                If another `Expression` instance is passed, it will be used as-is.
            properties: an optional mapping of table properties
            dialect: the dialect used to parse the input table.
            copy: if `False`, modify this expression instance in-place.
            opts: other options to use to parse the input table.

        Returns:
            The new Create expression.
        """
        instance = maybe_copy(self, copy)
        table_expression = maybe_parse(table, into=Table, dialect=dialect, **opts)

        properties_expression = None
        if properties:
            properties_expression = Properties.from_dict(properties)

        return Create(
            this=table_expression,
            kind="TABLE",
            expression=instance,
            properties=properties_expression,
        )

    def lock(self, update: bool = True, copy: bool = True) -> Select:
        """
        Set the locking read mode for this expression.

        Examples:
            >>> Select().select("x").from_("tbl").where("x = 'a'").lock().sql("mysql")
            "SELECT x FROM tbl WHERE x = 'a' FOR UPDATE"

            >>> Select().select("x").from_("tbl").where("x = 'a'").lock(update=False).sql("mysql")
            "SELECT x FROM tbl WHERE x = 'a' FOR SHARE"

        Args:
            update: if `True`, the locking type will be `FOR UPDATE`, else it will be `FOR SHARE`.
            copy: if `False`, modify this expression instance in-place.

        Returns:
            The modified expression.
        """
        inst = maybe_copy(self, copy)
        inst.set("locks", [Lock(update=update)])

        return inst

    def hint(self, *hints: ExpOrStr, dialect: DialectType = None, copy: bool = True) -> Select:
        """
        Set hints for this expression.

        Examples:
            >>> Select().select("x").from_("tbl").hint("BROADCAST(y)").sql(dialect="spark")
            'SELECT /*+ BROADCAST(y) */ x FROM tbl'

        Args:
            hints: The SQL code strings to parse as the hints.
                If an `Expression` instance is passed, it will be used as-is.
            dialect: The dialect used to parse the hints.
            copy: If `False`, modify this expression instance in-place.

        Returns:
            The modified expression.
        """
        inst = maybe_copy(self, copy)
        inst.set(
            "hint", Hint(expressions=[maybe_parse(h, copy=copy, dialect=dialect) for h in hints])
        )

        return inst

    @property
    def named_selects(self) -> t.List[str]:
        selects = []

        for e in self.expressions:
            if e.alias_or_name:
                selects.append(e.output_name)
            elif isinstance(e, Aliases):
                selects.extend([a.name for a in e.aliases])
        return selects

    @property
    def is_star(self) -> bool:
        return any(expression.is_star for expression in self.expressions)

    @property
    def selects(self) -> t.List[Expression]:
        return self.expressions


UNWRAPPED_QUERIES = (Select, SetOperation)


class Subquery(DerivedTable, Query):
    arg_types = {
        "this": True,
        "alias": False,
        "with_": False,
        **QUERY_MODIFIERS,
    }

    def unnest(self):
        """Returns the first non subquery."""
        expression = self
        while isinstance(expression, Subquery):
            expression = expression.this
        return expression

    def unwrap(self) -> Subquery:
        expression = self
        while expression.same_parent and expression.is_wrapper:
            expression = t.cast(Subquery, expression.parent)
        return expression

    def select(
        self,
        *expressions: t.Optional[ExpOrStr],
        append: bool = True,
        dialect: DialectType = None,
        copy: bool = True,
        **opts,
    ) -> Subquery:
        this = maybe_copy(self, copy)
        this.unnest().select(*expressions, append=append, dialect=dialect, copy=False, **opts)
        return this

    @property
    def is_wrapper(self) -> bool:
        """
        Whether this Subquery acts as a simple wrapper around another expression.

        SELECT * FROM (((SELECT * FROM t)))
                      ^
                      This corresponds to a "wrapper" Subquery node
        """
        return all(v is None for k, v in self.args.items() if k != "this")

    @property
    def is_star(self) -> bool:
        return self.this.is_star

    @property
    def output_name(self) -> str:
        return self.alias


class TimeUnit(Expression):
    """Automatically converts unit arg into a var."""

    arg_types = {"unit": False}

    UNABBREVIATED_UNIT_NAME = {
        "D": "DAY",
        "H": "HOUR",
        "M": "MINUTE",
        "MS": "MILLISECOND",
        "NS": "NANOSECOND",
        "Q": "QUARTER",
        "S": "SECOND",
        "US": "MICROSECOND",
        "W": "WEEK",
        "Y": "YEAR",
    }

    VAR_LIKE = (Column, Literal, Var)

    def __init__(self, **args):
        unit = args.get("unit")
        if type(unit) in self.VAR_LIKE and not (isinstance(unit, Column) and len(unit.parts) != 1):
            args["unit"] = Var(
                this=(self.UNABBREVIATED_UNIT_NAME.get(unit.name) or unit.name).upper()
            )
        elif isinstance(unit, Week):
            unit.set("this", Var(this=unit.this.name.upper()))

        super().__init__(**args)

    @property
    def unit(self) -> t.Optional[Var | IntervalSpan]:
        return self.args.get("unit")


class IntervalOp(TimeUnit):
    arg_types = {"unit": False, "expression": True}

    def interval(self):
        return Interval(
            this=self.expression.copy(),
            unit=self.unit.copy() if self.unit else None,
        )


# The `nullable` arg is helpful when transpiling types from other dialects to ClickHouse, which
# assumes non-nullable types by default. Values `None` and `True` mean the type is nullable.
class DataType(Expression):
    arg_types = {
        "this": True,
        "expressions": False,
        "nested": False,
        "values": False,
        "kind": False,
        "nullable": False,
    }

    class Type(AutoName):
        ARRAY = auto()
        AGGREGATEFUNCTION = auto()
        SIMPLEAGGREGATEFUNCTION = auto()
        BIGDECIMAL = auto()
        BIGINT = auto()
        BIGNUM = auto()
        BIGSERIAL = auto()
        BINARY = auto()
        BIT = auto()
        BLOB = auto()
        BOOLEAN = auto()
        BPCHAR = auto()
        CHAR = auto()
        DATE = auto()
        DATE32 = auto()
        DATEMULTIRANGE = auto()
        DATERANGE = auto()
        DATETIME = auto()
        DATETIME2 = auto()
        DATETIME64 = auto()
        DECIMAL = auto()
        DECIMAL32 = auto()
        DECIMAL64 = auto()
        DECIMAL128 = auto()
        DECIMAL256 = auto()
        DECFLOAT = auto()
        DOUBLE = auto()
        DYNAMIC = auto()
        ENUM = auto()
        ENUM8 = auto()
        ENUM16 = auto()
        FILE = auto()
        FIXEDSTRING = auto()
        FLOAT = auto()
        GEOGRAPHY = auto()
        GEOGRAPHYPOINT = auto()
        GEOMETRY = auto()
        POINT = auto()
        RING = auto()
        LINESTRING = auto()
        MULTILINESTRING = auto()
        POLYGON = auto()
        MULTIPOLYGON = auto()
        HLLSKETCH = auto()
        HSTORE = auto()
        IMAGE = auto()
        INET = auto()
        INT = auto()
        INT128 = auto()
        INT256 = auto()
        INT4MULTIRANGE = auto()
        INT4RANGE = auto()
        INT8MULTIRANGE = auto()
        INT8RANGE = auto()
        INTERVAL = auto()
        IPADDRESS = auto()
        IPPREFIX = auto()
        IPV4 = auto()
        IPV6 = auto()
        JSON = auto()  # noqa: F811
        JSONB = auto()
        LIST = auto()
        LONGBLOB = auto()
        LONGTEXT = auto()
        LOWCARDINALITY = auto()
        MAP = auto()
        MEDIUMBLOB = auto()
        MEDIUMINT = auto()
        MEDIUMTEXT = auto()
        MONEY = auto()
        NAME = auto()
        NCHAR = auto()
        NESTED = auto()
        NOTHING = auto()
        NULL = auto()
        NUMMULTIRANGE = auto()
        NUMRANGE = auto()
        NVARCHAR = auto()
        OBJECT = auto()
        RANGE = auto()
        ROWVERSION = auto()
        SERIAL = auto()
        SET = auto()
        SMALLDATETIME = auto()
        SMALLINT = auto()
        SMALLMONEY = auto()
        SMALLSERIAL = auto()
        STRUCT = auto()
        SUPER = auto()
        TEXT = auto()
        TINYBLOB = auto()
        TINYTEXT = auto()
        TIME = auto()
        TIMETZ = auto()
        TIME_NS = auto()
        TIMESTAMP = auto()
        TIMESTAMPNTZ = auto()
        TIMESTAMPLTZ = auto()
        TIMESTAMPTZ = auto()
        TIMESTAMP_S = auto()
        TIMESTAMP_MS = auto()
        TIMESTAMP_NS = auto()
        TINYINT = auto()
        TSMULTIRANGE = auto()
        TSRANGE = auto()
        TSTZMULTIRANGE = auto()
        TSTZRANGE = auto()
        UBIGINT = auto()
        UINT = auto()
        UINT128 = auto()
        UINT256 = auto()
        UMEDIUMINT = auto()
        UDECIMAL = auto()
        UDOUBLE = auto()
        UNION = auto()
        UNKNOWN = auto()  # Sentinel value, useful for type annotation
        USERDEFINED = "USER-DEFINED"
        USMALLINT = auto()
        UTINYINT = auto()
        UUID = auto()
        VARBINARY = auto()
        VARCHAR = auto()
        VARIANT = auto()
        VECTOR = auto()
        XML = auto()
        YEAR = auto()
        TDIGEST = auto()

    STRUCT_TYPES = {
        Type.FILE,
        Type.NESTED,
        Type.OBJECT,
        Type.STRUCT,
        Type.UNION,
    }

    ARRAY_TYPES = {
        Type.ARRAY,
        Type.LIST,
    }

    NESTED_TYPES = {
        *STRUCT_TYPES,
        *ARRAY_TYPES,
        Type.MAP,
    }

    TEXT_TYPES = {
        Type.CHAR,
        Type.NCHAR,
        Type.NVARCHAR,
        Type.TEXT,
        Type.VARCHAR,
        Type.NAME,
    }

    SIGNED_INTEGER_TYPES = {
        Type.BIGINT,
        Type.INT,
        Type.INT128,
        Type.INT256,
        Type.MEDIUMINT,
        Type.SMALLINT,
        Type.TINYINT,
    }

    UNSIGNED_INTEGER_TYPES = {
        Type.UBIGINT,
        Type.UINT,
        Type.UINT128,
        Type.UINT256,
        Type.UMEDIUMINT,
        Type.USMALLINT,
        Type.UTINYINT,
    }

    INTEGER_TYPES = {
        *SIGNED_INTEGER_TYPES,
        *UNSIGNED_INTEGER_TYPES,
        Type.BIT,
    }

    FLOAT_TYPES = {
        Type.DOUBLE,
        Type.FLOAT,
    }

    REAL_TYPES = {
        *FLOAT_TYPES,
        Type.BIGDECIMAL,
        Type.DECIMAL,
        Type.DECIMAL32,
        Type.DECIMAL64,
        Type.DECIMAL128,
        Type.DECIMAL256,
        Type.DECFLOAT,
        Type.MONEY,
        Type.SMALLMONEY,
        Type.UDECIMAL,
        Type.UDOUBLE,
    }

    NUMERIC_TYPES = {
        *INTEGER_TYPES,
        *REAL_TYPES,
    }

    TEMPORAL_TYPES = {
        Type.DATE,
        Type.DATE32,
        Type.DATETIME,
        Type.DATETIME2,
        Type.DATETIME64,
        Type.SMALLDATETIME,
        Type.TIME,
        Type.TIMESTAMP,
        Type.TIMESTAMPNTZ,
        Type.TIMESTAMPLTZ,
        Type.TIMESTAMPTZ,
        Type.TIMESTAMP_MS,
        Type.TIMESTAMP_NS,
        Type.TIMESTAMP_S,
        Type.TIMETZ,
    }

    @classmethod
    def build(
        cls,
        dtype: DATA_TYPE,
        dialect: DialectType = None,
        udt: bool = False,
        copy: bool = True,
        **kwargs,
    ) -> DataType:
        """
        Constructs a DataType object.

        Args:
            dtype: the data type of interest.
            dialect: the dialect to use for parsing `dtype`, in case it's a string.
            udt: when set to True, `dtype` will be used as-is if it can't be parsed into a
                DataType, thus creating a user-defined type.
            copy: whether to copy the data type.
            kwargs: additional arguments to pass in the constructor of DataType.

        Returns:
            The constructed DataType object.
        """
        from sqlglot import parse_one

        if isinstance(dtype, str):
            if dtype.upper() == "UNKNOWN":
                return DataType(this=DataType.Type.UNKNOWN, **kwargs)

            try:
                data_type_exp = parse_one(
                    dtype, read=dialect, into=DataType, error_level=ErrorLevel.IGNORE
                )
            except ParseError:
                if udt:
                    return DataType(this=DataType.Type.USERDEFINED, kind=dtype, **kwargs)
                raise
        elif isinstance(dtype, (Identifier, Dot)) and udt:
            return DataType(this=DataType.Type.USERDEFINED, kind=dtype, **kwargs)
        elif isinstance(dtype, DataType.Type):
            data_type_exp = DataType(this=dtype)
        elif isinstance(dtype, DataType):
            return maybe_copy(dtype, copy)
        else:
            raise ValueError(f"Invalid data type: {type(dtype)}. Expected str or DataType.Type")

        if kwargs:
            for k, v in kwargs.items():
                data_type_exp.set(k, v)
        return data_type_exp

    def is_type(self, *dtypes: DATA_TYPE, check_nullable: bool = False) -> bool:
        """
        Checks whether this DataType matches one of the provided data types. Nested types or precision
        will be compared using "structural equivalence" semantics, so e.g. array<int> != array<float>.

        Args:
            dtypes: the data types to compare this DataType to.
            check_nullable: whether to take the NULLABLE type constructor into account for the comparison.
                If false, it means that NULLABLE<INT> is equivalent to INT.

        Returns:
            True, if and only if there is a type in `dtypes` which is equal to this DataType.
        """
        self_is_nullable = self.args.get("nullable")
        for dtype in dtypes:
            other_type = DataType.build(dtype, copy=False, udt=True)
            other_is_nullable = other_type.args.get("nullable")
            if (
                other_type.expressions
                or (check_nullable and (self_is_nullable or other_is_nullable))
                or self.this == DataType.Type.USERDEFINED
                or other_type.this == DataType.Type.USERDEFINED
            ):
                matches = self == other_type
            else:
                matches = self.this == other_type.this

            if matches:
                return True
        return False


DATA_TYPE = t.Union[str, Identifier, Dot, DataType, DataType.Type]


# https://www.postgresql.org/docs/15/datatype-pseudo.html
class PseudoType(DataType):
    arg_types = {"this": True}


# https://www.postgresql.org/docs/15/datatype-oid.html
class ObjectIdentifier(DataType):
    arg_types = {"this": True}


# https://www.oracletutorial.com/oracle-basics/oracle-interval/
# https://trino.io/docs/current/language/types.html#interval-day-to-second
# https://docs.databricks.com/en/sql/language-manual/data-types/interval-type.html
class IntervalSpan(DataType):
    arg_types = {"this": True, "expression": True}


class Interval(TimeUnit):
    arg_types = {"this": False, "unit": False}


# https://docs.snowflake.com/en/sql-reference/functions/generator
class Generator(Func, UDTF):
    arg_types = {"rowcount": False, "timelimit": False}


class ArrayContains(Binary, Func):
    arg_types = {"this": True, "expression": True, "ensure_variant": False, "check_null": False}
    _sql_names = ["ARRAY_CONTAINS", "ARRAY_HAS"]


class ArrayContainsAll(Binary, Func):
    _sql_names = ["ARRAY_CONTAINS_ALL", "ARRAY_HAS_ALL"]


class ArrayOverlaps(Binary, Func):
    pass


class Case(Func):
    arg_types = {"this": False, "ifs": True, "default": False}

    def when(self, condition: ExpOrStr, then: ExpOrStr, copy: bool = True, **opts) -> Case:
        instance = maybe_copy(self, copy)
        instance.append(
            "ifs",
            If(
                this=maybe_parse(condition, copy=copy, **opts),
                true=maybe_parse(then, copy=copy, **opts),
            ),
        )
        return instance

    def else_(self, condition: ExpOrStr, copy: bool = True, **opts) -> Case:
        instance = maybe_copy(self, copy)
        instance.set("default", maybe_parse(condition, copy=copy, **opts))
        return instance


class Collate(Binary, Func):
    pass


class DateAdd(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateBin(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False, "zone": False, "origin": False}


class DateSub(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DateDiff(Func, TimeUnit):
    _sql_names = ["DATEDIFF", "DATE_DIFF"]
    arg_types = {
        "this": True,
        "expression": True,
        "unit": False,
        "zone": False,
        "big_int": False,
        "date_part_boundary": False,
    }


class DateTrunc(Func):
    arg_types = {"unit": True, "this": True, "zone": False, "input_type_preserved": False}

    def __init__(self, **args):
        # Across most dialects it's safe to unabbreviate the unit (e.g. 'Q' -> 'QUARTER') except Oracle
        # https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/ROUND-and-TRUNC-Date-Functions.html
        unabbreviate = args.pop("unabbreviate", True)

        unit = args.get("unit")
        if isinstance(unit, TimeUnit.VAR_LIKE) and not (
            isinstance(unit, Column) and len(unit.parts) != 1
        ):
            unit_name = unit.name.upper()
            if unabbreviate and unit_name in TimeUnit.UNABBREVIATED_UNIT_NAME:
                unit_name = TimeUnit.UNABBREVIATED_UNIT_NAME[unit_name]

            args["unit"] = Literal.string(unit_name)

        super().__init__(**args)

    @property
    def unit(self) -> Expression:
        return self.args["unit"]


class DatetimeAdd(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeSub(Func, IntervalOp):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class DatetimeTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


class LastDay(Func, TimeUnit):
    _sql_names = ["LAST_DAY", "LAST_DAY_OF_MONTH"]
    arg_types = {"this": True, "unit": False}


class Exists(Func, SubqueryPredicate):
    arg_types = {"this": True, "expression": False}


class TimestampAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampDiff(Func, TimeUnit):
    _sql_names = ["TIMESTAMPDIFF", "TIMESTAMP_DIFF"]
    arg_types = {"this": True, "expression": True, "unit": False}


class TimestampTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False, "input_type_preserved": False}


class TimeSlice(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": True, "kind": False}


class TimeAdd(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeSub(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class TimeTrunc(Func, TimeUnit):
    arg_types = {"this": True, "unit": True, "zone": False}


# https://docs.snowflake.com/en/sql-reference/functions/flatten
class Explode(Func, UDTF):
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True


class ExplodeOuter(Explode):
    pass


class Posexplode(Explode):
    pass


class PosexplodeOuter(Posexplode, ExplodeOuter):
    pass


class Unnest(Func, UDTF):
    arg_types = {
        "expressions": True,
        "alias": False,
        "offset": False,
        "explode_array": False,
    }

    @property
    def selects(self) -> t.List[Expression]:
        columns = super().selects
        offset = self.args.get("offset")
        if offset:
            columns = columns + [to_identifier("offset") if offset is True else offset]
        return columns


class And(Connector, Func):
    pass


class Or(Connector, Func):
    pass


class Xor(Connector, Func):
    arg_types = {"this": False, "expression": False, "expressions": False, "round_input": False}
    is_var_len_args = True


class JSONBContains(Binary, Func):
    _sql_names = ["JSONB_CONTAINS"]


# https://www.postgresql.org/docs/9.5/functions-json.html
class JSONBContainsAnyTopKeys(Binary, Func):
    pass


# https://www.postgresql.org/docs/9.5/functions-json.html
class JSONBContainsAllTopKeys(Binary, Func):
    pass


# https://www.postgresql.org/docs/9.5/functions-json.html
class JSONBDeleteAtPath(Binary, Func):
    pass


class JSONExtract(Binary, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "only_json_types": False,
        "expressions": False,
        "variant_extract": False,
        "json_query": False,
        "option": False,
        "quote": False,
        "on_condition": False,
        "requires_json": False,
        "emits": False,
    }
    _sql_names = ["JSON_EXTRACT"]
    is_var_len_args = True

    @property
    def output_name(self) -> str:
        return self.expression.output_name if not self.expressions else ""


class JSONExtractScalar(Binary, Func):
    arg_types = {
        "this": True,
        "expression": True,
        "only_json_types": False,
        "expressions": False,
        "json_type": False,
        "scalar_only": False,
    }
    _sql_names = ["JSON_EXTRACT_SCALAR"]
    is_var_len_args = True

    @property
    def output_name(self) -> str:
        return self.expression.output_name


class JSONBExtract(Binary, Func):
    _sql_names = ["JSONB_EXTRACT"]


class JSONBExtractScalar(Binary, Func):
    arg_types = {"this": True, "expression": True, "json_type": False}
    _sql_names = ["JSONB_EXTRACT_SCALAR"]


# https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#operator_member-of
class JSONArrayContains(Binary, Predicate, Func):
    arg_types = {"this": True, "expression": True, "json_type": False}
    _sql_names = ["JSON_ARRAY_CONTAINS"]


class Pow(Binary, Func):
    _sql_names = ["POWER", "POW"]


class RegexpLike(Binary, Func):
    arg_types = {"this": True, "expression": True, "flag": False, "full_match": False}


class RegexpILike(Binary, Func):
    arg_types = {"this": True, "expression": True, "flag": False}


class RegexpFullMatch(Binary, Func):
    arg_types = {"this": True, "expression": True, "options": False}


class TsOrDsAdd(Func, TimeUnit):
    # return_type is used to correctly cast the arguments of this expression when transpiling it
    arg_types = {"this": True, "expression": True, "unit": False, "return_type": False}

    @property
    def return_type(self) -> DataType:
        return DataType.build(self.args.get("return_type") or DataType.Type.DATE)


class TsOrDsDiff(Func, TimeUnit):
    arg_types = {"this": True, "expression": True, "unit": False}


class Corr(Binary, AggFunc):
    # Correlation divides by variance(column). If a column has 0 variance, the denominator
    # is 0 - some dialects return NaN (DuckDB) while others return NULL (Snowflake).
    # `null_on_zero_variance` is set to True at parse time for dialects that return NULL.
    arg_types = {"this": True, "expression": True, "null_on_zero_variance": False}


class Merge(DML):
    arg_types = {
        "this": True,
        "using": True,
        "on": False,
        "using_cond": False,
        "whens": True,
        "with_": False,
        "returning": False,
    }


ALL_FUNCTIONS = subclasses(__name__, Func, {AggFunc, Anonymous, Func})
FUNCTION_BY_NAME = {name: func for func in ALL_FUNCTIONS for name in func.sql_names()}

PERCENTILES = (PercentileCont, PercentileDisc)


# Helpers
@t.overload
def maybe_parse(
    sql_or_expression: ExpOrStr,
    *,
    into: t.Type[E],
    dialect: DialectType = None,
    prefix: t.Optional[str] = None,
    copy: bool = False,
    **opts,
) -> E: ...


@t.overload
def maybe_parse(
    sql_or_expression: str | E,
    *,
    into: t.Optional[IntoType] = None,
    dialect: DialectType = None,
    prefix: t.Optional[str] = None,
    copy: bool = False,
    **opts,
) -> E: ...


def maybe_parse(
    sql_or_expression: ExpOrStr,
    *,
    into: t.Optional[IntoType] = None,
    dialect: DialectType = None,
    prefix: t.Optional[str] = None,
    copy: bool = False,
    **opts,
) -> Expression:
    """Gracefully handle a possible string or expression.

    Example:
        >>> maybe_parse("1")
        Literal(this=1, is_string=False)
        >>> maybe_parse(to_identifier("x"))
        Identifier(this=x, quoted=False)

    Args:
        sql_or_expression: the SQL code string or an expression
        into: the SQLGlot Expression to parse into
        dialect: the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        prefix: a string to prefix the sql with before it gets parsed
            (automatically includes a space)
        copy: whether to copy the expression.
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Expression: the parsed or given expression.
    """
    if isinstance(sql_or_expression, Expression):
        if copy:
            return sql_or_expression.copy()
        return sql_or_expression

    if sql_or_expression is None:
        raise ParseError("SQL cannot be None")

    import sqlglot

    sql = str(sql_or_expression)
    if prefix:
        sql = f"{prefix} {sql}"

    return sqlglot.parse_one(sql, read=dialect, into=into, **opts)


@t.overload
def maybe_copy(instance: None, copy: bool = True) -> None: ...


@t.overload
def maybe_copy(instance: E, copy: bool = True) -> E: ...


def maybe_copy(instance, copy=True):
    return instance.copy() if copy and instance else instance


def _to_s(node: t.Any, verbose: bool = False, level: int = 0, repr_str: bool = False) -> str:
    """Generate a textual representation of an Expression tree"""
    indent = "\n" + ("  " * (level + 1))
    delim = f",{indent}"

    if isinstance(node, Expression):
        args = {k: v for k, v in node.args.items() if (v is not None and v != []) or verbose}

        if (node.type or verbose) and not isinstance(node, DataType):
            args["_type"] = node.type
        if node.comments or verbose:
            args["_comments"] = node.comments

        if verbose:
            args["_id"] = id(node)

        # Inline leaves for a more compact representation
        if node.is_leaf():
            indent = ""
            delim = ", "

        repr_str = node.is_string or (isinstance(node, Identifier) and node.quoted)
        items = delim.join(
            [f"{k}={_to_s(v, verbose, level + 1, repr_str=repr_str)}" for k, v in args.items()]
        )
        return f"{node.__class__.__name__}({indent}{items})"

    if isinstance(node, list):
        items = delim.join(_to_s(i, verbose, level + 1) for i in node)
        items = f"{indent}{items}" if items else ""
        return f"[{items}]"

    # We use the representation of the string to avoid stripping out important whitespace
    if repr_str and isinstance(node, str):
        node = repr(node)

    # Indent multiline strings to match the current level
    return indent.join(textwrap.dedent(str(node).strip("\n")).splitlines())


def _is_wrong_expression(expression, into):
    return isinstance(expression, Expression) and not isinstance(expression, into)


def _apply_builder(
    expression,
    instance,
    arg,
    copy=True,
    prefix=None,
    into=None,
    dialect=None,
    into_arg="this",
    **opts,
):
    if _is_wrong_expression(expression, into):
        expression = into(**{into_arg: expression})
    instance = maybe_copy(instance, copy)
    expression = maybe_parse(
        sql_or_expression=expression,
        prefix=prefix,
        into=into,
        dialect=dialect,
        **opts,
    )
    instance.set(arg, expression)
    return instance


def _apply_child_list_builder(
    *expressions,
    instance,
    arg,
    append=True,
    copy=True,
    prefix=None,
    into=None,
    dialect=None,
    properties=None,
    **opts,
):
    instance = maybe_copy(instance, copy)
    parsed = []
    properties = {} if properties is None else properties

    for expression in expressions:
        if expression is not None:
            if _is_wrong_expression(expression, into):
                expression = into(expressions=[expression])

            expression = maybe_parse(
                expression,
                into=into,
                dialect=dialect,
                prefix=prefix,
                **opts,
            )
            for k, v in expression.args.items():
                if k == "expressions":
                    parsed.extend(v)
                else:
                    properties[k] = v

    existing = instance.args.get(arg)
    if append and existing:
        parsed = existing.expressions + parsed

    child = into(expressions=parsed)
    for k, v in properties.items():
        child.set(k, v)
    instance.set(arg, child)

    return instance


def _apply_list_builder(
    *expressions,
    instance,
    arg,
    append=True,
    copy=True,
    prefix=None,
    into=None,
    dialect=None,
    **opts,
):
    inst = maybe_copy(instance, copy)

    expressions = [
        maybe_parse(
            sql_or_expression=expression,
            into=into,
            prefix=prefix,
            dialect=dialect,
            **opts,
        )
        for expression in expressions
        if expression is not None
    ]

    existing_expressions = inst.args.get(arg)
    if append and existing_expressions:
        expressions = existing_expressions + expressions

    inst.set(arg, expressions)
    return inst


def _apply_conjunction_builder(
    *expressions,
    instance,
    arg,
    into=None,
    append=True,
    copy=True,
    dialect=None,
    **opts,
):
    expressions = [exp for exp in expressions if exp is not None and exp != ""]
    if not expressions:
        return instance

    inst = maybe_copy(instance, copy)

    existing = inst.args.get(arg)
    if append and existing is not None:
        expressions = [existing.this if into else existing] + list(expressions)

    node = and_(*expressions, dialect=dialect, copy=copy, **opts)

    inst.set(arg, into(this=node) if into else node)
    return inst


def _apply_cte_builder(
    instance: E,
    alias: ExpOrStr,
    as_: ExpOrStr,
    recursive: t.Optional[bool] = None,
    materialized: t.Optional[bool] = None,
    append: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    scalar: t.Optional[bool] = None,
    **opts,
) -> E:
    alias_expression = maybe_parse(alias, dialect=dialect, into=TableAlias, **opts)
    as_expression = maybe_parse(as_, dialect=dialect, copy=copy, **opts)
    if scalar and not isinstance(as_expression, Subquery):
        # scalar CTE must be wrapped in a subquery
        as_expression = Subquery(this=as_expression)
    cte = CTE(this=as_expression, alias=alias_expression, materialized=materialized, scalar=scalar)
    return _apply_child_list_builder(
        cte,
        instance=instance,
        arg="with_",
        append=append,
        copy=copy,
        into=With,
        properties={"recursive": recursive} if recursive else {},
    )


def _combine(
    expressions: t.Sequence[t.Optional[ExpOrStr]],
    operator: t.Type[Connector],
    dialect: DialectType = None,
    copy: bool = True,
    wrap: bool = True,
    **opts,
) -> Expression:
    conditions = [
        condition(expression, dialect=dialect, copy=copy, **opts)
        for expression in expressions
        if expression is not None
    ]

    this, *rest = conditions
    if rest and wrap:
        this = _wrap(this, Connector)
    for expression in rest:
        this = operator(this=this, expression=_wrap(expression, Connector) if wrap else expression)

    return this


@t.overload
def _wrap(expression: None, kind: t.Type[Expression]) -> None: ...


@t.overload
def _wrap(expression: E, kind: t.Type[Expression]) -> E | Paren: ...


def _wrap(expression: t.Optional[E], kind: t.Type[Expression]) -> t.Optional[E] | Paren:
    return Paren(this=expression) if isinstance(expression, kind) else expression


def _apply_set_operation(
    *expressions: ExpOrStr,
    set_operation: t.Type[S],
    distinct: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> S:
    return reduce(
        lambda x, y: set_operation(this=x, expression=y, distinct=distinct, **opts),
        (maybe_parse(e, dialect=dialect, copy=copy, **opts) for e in expressions),
    )


def union(
    *expressions: ExpOrStr,
    distinct: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Union:
    """
    Initializes a syntax tree for the `UNION` operation.

    Example:
        >>> union("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo UNION SELECT * FROM bla'

    Args:
        expressions: the SQL code strings, corresponding to the `UNION`'s operands.
            If `Expression` instances are passed, they will be used as-is.
        distinct: set the DISTINCT flag if and only if this is true.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy the expression.
        opts: other options to use to parse the input expressions.

    Returns:
        The new Union instance.
    """
    assert len(expressions) >= 2, "At least two expressions are required by `union`."
    return _apply_set_operation(
        *expressions, set_operation=Union, distinct=distinct, dialect=dialect, copy=copy, **opts
    )


def intersect(
    *expressions: ExpOrStr,
    distinct: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Intersect:
    """
    Initializes a syntax tree for the `INTERSECT` operation.

    Example:
        >>> intersect("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo INTERSECT SELECT * FROM bla'

    Args:
        expressions: the SQL code strings, corresponding to the `INTERSECT`'s operands.
            If `Expression` instances are passed, they will be used as-is.
        distinct: set the DISTINCT flag if and only if this is true.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy the expression.
        opts: other options to use to parse the input expressions.

    Returns:
        The new Intersect instance.
    """
    assert len(expressions) >= 2, "At least two expressions are required by `intersect`."
    return _apply_set_operation(
        *expressions, set_operation=Intersect, distinct=distinct, dialect=dialect, copy=copy, **opts
    )


def except_(
    *expressions: ExpOrStr,
    distinct: bool = True,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Except:
    """
    Initializes a syntax tree for the `EXCEPT` operation.

    Example:
        >>> except_("SELECT * FROM foo", "SELECT * FROM bla").sql()
        'SELECT * FROM foo EXCEPT SELECT * FROM bla'

    Args:
        expressions: the SQL code strings, corresponding to the `EXCEPT`'s operands.
            If `Expression` instances are passed, they will be used as-is.
        distinct: set the DISTINCT flag if and only if this is true.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy the expression.
        opts: other options to use to parse the input expressions.

    Returns:
        The new Except instance.
    """
    assert len(expressions) >= 2, "At least two expressions are required by `except_`."
    return _apply_set_operation(
        *expressions, set_operation=Except, distinct=distinct, dialect=dialect, copy=copy, **opts
    )


def select(*expressions: ExpOrStr, dialect: DialectType = None, **opts) -> Select:
    """
    Initializes a syntax tree from one or multiple SELECT expressions.

    Example:
        >>> select("col1", "col2").from_("tbl").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expressions: the SQL code string to parse as the expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expressions (in the case that an
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that an input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().select(*expressions, dialect=dialect, **opts)


def from_(expression: ExpOrStr, dialect: DialectType = None, **opts) -> Select:
    """
    Initializes a syntax tree from a FROM expression.

    Example:
        >>> from_("tbl").select("col1", "col2").sql()
        'SELECT col1, col2 FROM tbl'

    Args:
        *expression: the SQL code string to parse as the FROM expressions of a
            SELECT statement. If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        Select: the syntax tree for the SELECT statement.
    """
    return Select().from_(expression, dialect=dialect, **opts)


def update(
    table: str | Table,
    properties: t.Optional[dict] = None,
    where: t.Optional[ExpOrStr] = None,
    from_: t.Optional[ExpOrStr] = None,
    with_: t.Optional[t.Dict[str, ExpOrStr]] = None,
    dialect: DialectType = None,
    **opts,
) -> Update:
    """
    Creates an update statement.

    Example:
        >>> update("my_table", {"x": 1, "y": "2", "z": None}, from_="baz_cte", where="baz_cte.id > 1 and my_table.id = baz_cte.id", with_={"baz_cte": "SELECT id FROM foo"}).sql()
        "WITH baz_cte AS (SELECT id FROM foo) UPDATE my_table SET x = 1, y = '2', z = NULL FROM baz_cte WHERE baz_cte.id > 1 AND my_table.id = baz_cte.id"

    Args:
        properties: dictionary of properties to SET which are
            auto converted to sql objects eg None -> NULL
        where: sql conditional parsed into a WHERE statement
        from_: sql statement parsed into a FROM statement
        with_: dictionary of CTE aliases / select statements to include in a WITH clause.
        dialect: the dialect used to parse the input expressions.
        **opts: other options to use to parse the input expressions.

    Returns:
        Update: the syntax tree for the UPDATE statement.
    """
    update_expr = Update(this=maybe_parse(table, into=Table, dialect=dialect))
    if properties:
        update_expr.set(
            "expressions",
            [
                EQ(this=maybe_parse(k, dialect=dialect, **opts), expression=convert(v))
                for k, v in properties.items()
            ],
        )
    if from_:
        update_expr.set(
            "from_",
            maybe_parse(from_, into=From, dialect=dialect, prefix="FROM", **opts),
        )
    if isinstance(where, Condition):
        where = Where(this=where)
    if where:
        update_expr.set(
            "where",
            maybe_parse(where, into=Where, dialect=dialect, prefix="WHERE", **opts),
        )
    if with_:
        cte_list = [
            alias_(CTE(this=maybe_parse(qry, dialect=dialect, **opts)), alias, table=True)
            for alias, qry in with_.items()
        ]
        update_expr.set(
            "with_",
            With(expressions=cte_list),
        )
    return update_expr


def delete(
    table: ExpOrStr,
    where: t.Optional[ExpOrStr] = None,
    returning: t.Optional[ExpOrStr] = None,
    dialect: DialectType = None,
    **opts,
) -> Delete:
    """
    Builds a delete statement.

    Example:
        >>> delete("my_table", where="id > 1").sql()
        'DELETE FROM my_table WHERE id > 1'

    Args:
        where: sql conditional parsed into a WHERE statement
        returning: sql conditional parsed into a RETURNING statement
        dialect: the dialect used to parse the input expressions.
        **opts: other options to use to parse the input expressions.

    Returns:
        Delete: the syntax tree for the DELETE statement.
    """
    delete_expr = Delete().delete(table, dialect=dialect, copy=False, **opts)
    if where:
        delete_expr = delete_expr.where(where, dialect=dialect, copy=False, **opts)
    if returning:
        delete_expr = delete_expr.returning(returning, dialect=dialect, copy=False, **opts)
    return delete_expr


def insert(
    expression: ExpOrStr,
    into: ExpOrStr,
    columns: t.Optional[t.Sequence[str | Identifier]] = None,
    overwrite: t.Optional[bool] = None,
    returning: t.Optional[ExpOrStr] = None,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Insert:
    """
    Builds an INSERT statement.

    Example:
        >>> insert("VALUES (1, 2, 3)", "tbl").sql()
        'INSERT INTO tbl VALUES (1, 2, 3)'

    Args:
        expression: the sql string or expression of the INSERT statement
        into: the tbl to insert data to.
        columns: optionally the table's column names.
        overwrite: whether to INSERT OVERWRITE or not.
        returning: sql conditional parsed into a RETURNING statement
        dialect: the dialect used to parse the input expressions.
        copy: whether to copy the expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Insert: the syntax tree for the INSERT statement.
    """
    expr = maybe_parse(expression, dialect=dialect, copy=copy, **opts)
    this: Table | Schema = maybe_parse(into, into=Table, dialect=dialect, copy=copy, **opts)

    if columns:
        this = Schema(this=this, expressions=[to_identifier(c, copy=copy) for c in columns])

    insert = Insert(this=this, expression=expr, overwrite=overwrite)

    if returning:
        insert = insert.returning(returning, dialect=dialect, copy=False, **opts)

    return insert


def merge(
    *when_exprs: ExpOrStr,
    into: ExpOrStr,
    using: ExpOrStr,
    on: ExpOrStr,
    returning: t.Optional[ExpOrStr] = None,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
) -> Merge:
    """
    Builds a MERGE statement.

    Example:
        >>> merge("WHEN MATCHED THEN UPDATE SET col1 = source_table.col1",
        ...       "WHEN NOT MATCHED THEN INSERT (col1) VALUES (source_table.col1)",
        ...       into="my_table",
        ...       using="source_table",
        ...       on="my_table.id = source_table.id").sql()
        'MERGE INTO my_table USING source_table ON my_table.id = source_table.id WHEN MATCHED THEN UPDATE SET col1 = source_table.col1 WHEN NOT MATCHED THEN INSERT (col1) VALUES (source_table.col1)'

    Args:
        *when_exprs: The WHEN clauses specifying actions for matched and unmatched rows.
        into: The target table to merge data into.
        using: The source table to merge data from.
        on: The join condition for the merge.
        returning: The columns to return from the merge.
        dialect: The dialect used to parse the input expressions.
        copy: Whether to copy the expression.
        **opts: Other options to use to parse the input expressions.

    Returns:
        Merge: The syntax tree for the MERGE statement.
    """
    expressions: t.List[Expression] = []
    for when_expr in when_exprs:
        expression = maybe_parse(when_expr, dialect=dialect, copy=copy, into=Whens, **opts)
        expressions.extend([expression] if isinstance(expression, When) else expression.expressions)

    merge = Merge(
        this=maybe_parse(into, dialect=dialect, copy=copy, **opts),
        using=maybe_parse(using, dialect=dialect, copy=copy, **opts),
        on=maybe_parse(on, dialect=dialect, copy=copy, **opts),
        whens=Whens(expressions=expressions),
    )
    if returning:
        merge = merge.returning(returning, dialect=dialect, copy=False, **opts)

    if isinstance(using_clause := merge.args.get("using"), Alias):
        using_clause.replace(alias_(using_clause.this, using_clause.args["alias"], table=True))

    return merge


def condition(
    expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts
) -> Condition:
    """
    Initialize a logical condition expression.

    Example:
        >>> condition("x=1").sql()
        'x = 1'

        This is helpful for composing larger logical syntax trees:
        >>> where = condition("x=1")
        >>> where = where.and_("y=1")
        >>> Select().from_("tbl").select("*").where(where).sql()
        'SELECT * FROM tbl WHERE x = 1 AND y = 1'

    Args:
        *expression: the SQL code string to parse.
            If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression (in the case that the
            input expression is a SQL string).
        copy: Whether to copy `expression` (only applies to expressions).
        **opts: other options to use to parse the input expressions (again, in the case
            that the input expression is a SQL string).

    Returns:
        The new Condition instance
    """
    return maybe_parse(
        expression,
        into=Condition,
        dialect=dialect,
        copy=copy,
        **opts,
    )


def and_(
    *expressions: t.Optional[ExpOrStr],
    dialect: DialectType = None,
    copy: bool = True,
    wrap: bool = True,
    **opts,
) -> Condition:
    """
    Combine multiple conditions with an AND logical operator.

    Example:
        >>> and_("x=1", and_("y=1", "z=1")).sql()
        'x = 1 AND (y = 1 AND z = 1)'

    Args:
        *expressions: the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy `expressions` (only applies to Expressions).
        wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
            precedence issues, but can be turned off when the produced AST is too deep and
            causes recursion-related issues.
        **opts: other options to use to parse the input expressions.

    Returns:
        The new condition
    """
    return t.cast(Condition, _combine(expressions, And, dialect, copy=copy, wrap=wrap, **opts))


def or_(
    *expressions: t.Optional[ExpOrStr],
    dialect: DialectType = None,
    copy: bool = True,
    wrap: bool = True,
    **opts,
) -> Condition:
    """
    Combine multiple conditions with an OR logical operator.

    Example:
        >>> or_("x=1", or_("y=1", "z=1")).sql()
        'x = 1 OR (y = 1 OR z = 1)'

    Args:
        *expressions: the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy `expressions` (only applies to Expressions).
        wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
            precedence issues, but can be turned off when the produced AST is too deep and
            causes recursion-related issues.
        **opts: other options to use to parse the input expressions.

    Returns:
        The new condition
    """
    return t.cast(Condition, _combine(expressions, Or, dialect, copy=copy, wrap=wrap, **opts))


def xor(
    *expressions: t.Optional[ExpOrStr],
    dialect: DialectType = None,
    copy: bool = True,
    wrap: bool = True,
    **opts,
) -> Condition:
    """
    Combine multiple conditions with an XOR logical operator.

    Example:
        >>> xor("x=1", xor("y=1", "z=1")).sql()
        'x = 1 XOR (y = 1 XOR z = 1)'

    Args:
        *expressions: the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy `expressions` (only applies to Expressions).
        wrap: whether to wrap the operands in `Paren`s. This is true by default to avoid
            precedence issues, but can be turned off when the produced AST is too deep and
            causes recursion-related issues.
        **opts: other options to use to parse the input expressions.

    Returns:
        The new condition
    """
    return t.cast(Condition, _combine(expressions, Xor, dialect, copy=copy, wrap=wrap, **opts))


def not_(expression: ExpOrStr, dialect: DialectType = None, copy: bool = True, **opts) -> Not:
    """
    Wrap a condition with a NOT operator.

    Example:
        >>> not_("this_suit='black'").sql()
        "NOT this_suit = 'black'"

    Args:
        expression: the SQL code string to parse.
            If an Expression instance is passed, this is used as-is.
        dialect: the dialect used to parse the input expression.
        copy: whether to copy the expression or not.
        **opts: other options to use to parse the input expressions.

    Returns:
        The new condition.
    """
    this = condition(
        expression,
        dialect=dialect,
        copy=copy,
        **opts,
    )
    return Not(this=_wrap(this, Connector))


def paren(expression: ExpOrStr, copy: bool = True) -> Paren:
    """
    Wrap an expression in parentheses.

    Example:
        >>> paren("5 + 3").sql()
        '(5 + 3)'

    Args:
        expression: the SQL code string to parse.
            If an Expression instance is passed, this is used as-is.
        copy: whether to copy the expression or not.

    Returns:
        The wrapped expression.
    """
    return Paren(this=maybe_parse(expression, copy=copy))


SAFE_IDENTIFIER_RE: t.Pattern[str] = re.compile(r"^[_a-zA-Z][\w]*$")


@t.overload
def to_identifier(name: None, quoted: t.Optional[bool] = None, copy: bool = True) -> None: ...


@t.overload
def to_identifier(
    name: str | Identifier, quoted: t.Optional[bool] = None, copy: bool = True
) -> Identifier: ...


def to_identifier(name, quoted=None, copy=True):
    """Builds an identifier.

    Args:
        name: The name to turn into an identifier.
        quoted: Whether to force quote the identifier.
        copy: Whether to copy name if it's an Identifier.

    Returns:
        The identifier ast node.
    """

    if name is None:
        return None

    if isinstance(name, Identifier):
        identifier = maybe_copy(name, copy)
    elif isinstance(name, str):
        identifier = Identifier(
            this=name,
            quoted=not SAFE_IDENTIFIER_RE.match(name) if quoted is None else quoted,
        )
    else:
        raise ValueError(f"Name needs to be a string or an Identifier, got: {name.__class__}")
    return identifier


def parse_identifier(name: str | Identifier, dialect: DialectType = None) -> Identifier:
    """
    Parses a given string into an identifier.

    Args:
        name: The name to parse into an identifier.
        dialect: The dialect to parse against.

    Returns:
        The identifier ast node.
    """
    try:
        expression = maybe_parse(name, dialect=dialect, into=Identifier)
    except (ParseError, TokenError):
        expression = to_identifier(name)

    return expression


INTERVAL_STRING_RE = re.compile(r"\s*(-?[0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)\s*")

# Matches day-time interval strings that contain
# - A number of days (possibly negative or with decimals)
# - At least one space
# - Portions of a time-like signature, potentially negative
#   - Standard format                   [-]h+:m+:s+[.f+]
#   - Just minutes/seconds/frac seconds [-]m+:s+.f+
#   - Just hours, minutes, maybe colon  [-]h+:m+[:]
#   - Just hours, maybe colon           [-]h+[:]
#   - Just colon                        :
INTERVAL_DAY_TIME_RE = re.compile(
    r"\s*-?\s*\d+(?:\.\d+)?\s+(?:-?(?:\d+:)?\d+:\d+(?:\.\d+)?|-?(?:\d+:){1,2}|:)\s*"
)


def to_interval(interval: str | Literal) -> Interval:
    """Builds an interval expression from a string like '1 day' or '5 months'."""
    if isinstance(interval, Literal):
        if not interval.is_string:
            raise ValueError("Invalid interval string.")

        interval = interval.this

    interval = maybe_parse(f"INTERVAL {interval}")
    assert isinstance(interval, Interval)
    return interval


def to_table(
    sql_path: str | Table, dialect: DialectType = None, copy: bool = True, **kwargs
) -> Table:
    """
    Create a table expression from a `[catalog].[schema].[table]` sql path. Catalog and schema are optional.
    If a table is passed in then that table is returned.

    Args:
        sql_path: a `[catalog].[schema].[table]` string.
        dialect: the source dialect according to which the table name will be parsed.
        copy: Whether to copy a table if it is passed in.
        kwargs: the kwargs to instantiate the resulting `Table` expression with.

    Returns:
        A table expression.
    """
    if isinstance(sql_path, Table):
        return maybe_copy(sql_path, copy=copy)

    try:
        table = maybe_parse(sql_path, into=Table, dialect=dialect)
    except ParseError:
        catalog, db, this = split_num_words(sql_path, ".", 3)

        if not this:
            raise

        table = table_(this, db=db, catalog=catalog)

    for k, v in kwargs.items():
        table.set(k, v)

    return table


def to_column(
    sql_path: str | Column,
    quoted: t.Optional[bool] = None,
    dialect: DialectType = None,
    copy: bool = True,
    **kwargs,
) -> Column:
    """
    Create a column from a `[table].[column]` sql path. Table is optional.
    If a column is passed in then that column is returned.

    Args:
        sql_path: a `[table].[column]` string.
        quoted: Whether or not to force quote identifiers.
        dialect: the source dialect according to which the column name will be parsed.
        copy: Whether to copy a column if it is passed in.
        kwargs: the kwargs to instantiate the resulting `Column` expression with.

    Returns:
        A column expression.
    """
    if isinstance(sql_path, Column):
        return maybe_copy(sql_path, copy=copy)

    try:
        col = maybe_parse(sql_path, into=Column, dialect=dialect)
    except ParseError:
        return column(*reversed(sql_path.split(".")), quoted=quoted, **kwargs)

    for k, v in kwargs.items():
        col.set(k, v)

    if quoted:
        for i in col.find_all(Identifier):
            i.set("quoted", True)

    return col


def alias_(
    expression: ExpOrStr,
    alias: t.Optional[str | Identifier],
    table: bool | t.Sequence[str | Identifier] = False,
    quoted: t.Optional[bool] = None,
    dialect: DialectType = None,
    copy: bool = True,
    **opts,
):
    """Create an Alias expression.

    Example:
        >>> alias_('foo', 'bar').sql()
        'foo AS bar'

        >>> alias_('(select 1, 2)', 'bar', table=['a', 'b']).sql()
        '(SELECT 1, 2) AS bar(a, b)'

    Args:
        expression: the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        alias: the alias name to use. If the name has
            special characters it is quoted.
        table: Whether to create a table alias, can also be a list of columns.
        quoted: whether to quote the alias
        dialect: the dialect used to parse the input expression.
        copy: Whether to copy the expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        Alias: the aliased expression
    """
    exp = maybe_parse(expression, dialect=dialect, copy=copy, **opts)
    alias = to_identifier(alias, quoted=quoted)

    if table:
        table_alias = TableAlias(this=alias)
        exp.set("alias", table_alias)

        if not isinstance(table, bool):
            for column in table:
                table_alias.append("columns", to_identifier(column, quoted=quoted))

        return exp

    # We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
    # the AST, representing a "named_window" [1] construct (eg. bigquery). What we want is an ALIAS node
    # for the complete Window expression.
    #
    # [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls

    if "alias" in exp.arg_types and not isinstance(exp, Window):
        exp.set("alias", alias)
        return exp
    return Alias(this=exp, alias=alias)


def subquery(
    expression: ExpOrStr,
    alias: t.Optional[Identifier | str] = None,
    dialect: DialectType = None,
    **opts,
) -> Select:
    """
    Build a subquery expression that's selected from.

    Example:
        >>> subquery('select x from tbl', 'bar').select('x').sql()
        'SELECT x FROM (SELECT x FROM tbl) AS bar'

    Args:
        expression: the SQL code strings to parse.
            If an Expression instance is passed, this is used as-is.
        alias: the alias name to use.
        dialect: the dialect used to parse the input expression.
        **opts: other options to use to parse the input expressions.

    Returns:
        A new Select instance with the subquery expression included.
    """

    expression = maybe_parse(expression, dialect=dialect, **opts).subquery(alias, **opts)
    return Select().from_(expression, dialect=dialect, **opts)


@t.overload
def column(
    col: str | Identifier,
    table: t.Optional[str | Identifier] = None,
    db: t.Optional[str | Identifier] = None,
    catalog: t.Optional[str | Identifier] = None,
    *,
    fields: t.Collection[t.Union[str, Identifier]],
    quoted: t.Optional[bool] = None,
    copy: bool = True,
) -> Dot:
    pass


@t.overload
def column(
    col: str | Identifier | Star,
    table: t.Optional[str | Identifier] = None,
    db: t.Optional[str | Identifier] = None,
    catalog: t.Optional[str | Identifier] = None,
    *,
    fields: Lit[None] = None,
    quoted: t.Optional[bool] = None,
    copy: bool = True,
) -> Column:
    pass


def column(
    col,
    table=None,
    db=None,
    catalog=None,
    *,
    fields=None,
    quoted=None,
    copy=True,
):
    """
    Build a Column.

    Args:
        col: Column name.
        table: Table name.
        db: Database name.
        catalog: Catalog name.
        fields: Additional fields using dots.
        quoted: Whether to force quotes on the column's identifiers.
        copy: Whether to copy identifiers if passed in.

    Returns:
        The new Column instance.
    """
    if not isinstance(col, Star):
        col = to_identifier(col, quoted=quoted, copy=copy)

    this = Column(
        this=col,
        table=to_identifier(table, quoted=quoted, copy=copy),
        db=to_identifier(db, quoted=quoted, copy=copy),
        catalog=to_identifier(catalog, quoted=quoted, copy=copy),
    )

    if fields:
        this = Dot.build(
            (this, *(to_identifier(field, quoted=quoted, copy=copy) for field in fields))
        )
    return this


def cast(
    expression: ExpOrStr, to: DATA_TYPE, copy: bool = True, dialect: DialectType = None, **opts
) -> Cast:
    """Cast an expression to a data type.

    Example:
        >>> cast('x + 1', 'int').sql()
        'CAST(x + 1 AS INT)'

    Args:
        expression: The expression to cast.
        to: The datatype to cast to.
        copy: Whether to copy the supplied expressions.
        dialect: The target dialect. This is used to prevent a re-cast in the following scenario:
            - The expression to be cast is already a exp.Cast expression
            - The existing cast is to a type that is logically equivalent to new type

            For example, if :expression='CAST(x as DATETIME)' and :to=Type.TIMESTAMP,
            but in the target dialect DATETIME is mapped to TIMESTAMP, then we will NOT return `CAST(x (as DATETIME) as TIMESTAMP)`
            and instead just return the original expression `CAST(x as DATETIME)`.

            This is to prevent it being output as a double cast `CAST(x (as TIMESTAMP) as TIMESTAMP)` once the DATETIME -> TIMESTAMP
            mapping is applied in the target dialect generator.

    Returns:
        The new Cast instance.
    """
    expr = maybe_parse(expression, copy=copy, dialect=dialect, **opts)
    data_type = DataType.build(to, copy=copy, dialect=dialect, **opts)

    # dont re-cast if the expression is already a cast to the correct type
    if isinstance(expr, Cast):
        from sqlglot.dialects.dialect import Dialect

        target_dialect = Dialect.get_or_raise(dialect)
        type_mapping = target_dialect.generator_class.TYPE_MAPPING

        existing_cast_type: DataType.Type = expr.to.this
        new_cast_type: DataType.Type = data_type.this
        types_are_equivalent = type_mapping.get(
            existing_cast_type, existing_cast_type.value
        ) == type_mapping.get(new_cast_type, new_cast_type.value)

        if expr.is_type(data_type) or types_are_equivalent:
            return expr

    expr = Cast(this=expr, to=data_type)
    expr.type = data_type

    return expr


def table_(
    table: Identifier | str,
    db: t.Optional[Identifier | str] = None,
    catalog: t.Optional[Identifier | str] = None,
    quoted: t.Optional[bool] = None,
    alias: t.Optional[Identifier | str] = None,
) -> Table:
    """Build a Table.

    Args:
        table: Table name.
        db: Database name.
        catalog: Catalog name.
        quote: Whether to force quotes on the table's identifiers.
        alias: Table's alias.

    Returns:
        The new Table instance.
    """
    return Table(
        this=to_identifier(table, quoted=quoted) if table else None,
        db=to_identifier(db, quoted=quoted) if db else None,
        catalog=to_identifier(catalog, quoted=quoted) if catalog else None,
        alias=TableAlias(this=to_identifier(alias)) if alias else None,
    )


def values(
    values: t.Iterable[t.Tuple[t.Any, ...]],
    alias: t.Optional[str] = None,
    columns: t.Optional[t.Iterable[str] | t.Dict[str, DataType]] = None,
) -> Values:
    """Build VALUES statement.

    Example:
        >>> values([(1, '2')]).sql()
        "VALUES (1, '2')"

    Args:
        values: values statements that will be converted to SQL
        alias: optional alias
        columns: Optional list of ordered column names or ordered dictionary of column names to types.
         If either are provided then an alias is also required.

    Returns:
        Values: the Values expression object
    """
    if columns and not alias:
        raise ValueError("Alias is required when providing columns")

    return Values(
        expressions=[convert(tup) for tup in values],
        alias=(
            TableAlias(this=to_identifier(alias), columns=[to_identifier(x) for x in columns])
            if columns
            else (TableAlias(this=to_identifier(alias)) if alias else None)
        ),
    )


def var(name: t.Optional[ExpOrStr]) -> Var:
    """Build a SQL variable.

    Example:
        >>> repr(var('x'))
        'Var(this=x)'

        >>> repr(var(column('x', table='y')))
        'Var(this=x)'

    Args:
        name: The name of the var or an expression who's name will become the var.

    Returns:
        The new variable node.
    """
    if not name:
        raise ValueError("Cannot convert empty name into var.")

    if isinstance(name, Expression):
        name = name.name
    return Var(this=name)


def rename_table(
    old_name: str | Table,
    new_name: str | Table,
    dialect: DialectType = None,
) -> Alter:
    """Build ALTER TABLE... RENAME... expression

    Args:
        old_name: The old name of the table
        new_name: The new name of the table
        dialect: The dialect to parse the table.

    Returns:
        Alter table expression
    """
    old_table = to_table(old_name, dialect=dialect)
    new_table = to_table(new_name, dialect=dialect)
    return Alter(
        this=old_table,
        kind="TABLE",
        actions=[
            AlterRename(this=new_table),
        ],
    )


def rename_column(
    table_name: str | Table,
    old_column_name: str | Column,
    new_column_name: str | Column,
    exists: t.Optional[bool] = None,
    dialect: DialectType = None,
) -> Alter:
    """Build ALTER TABLE... RENAME COLUMN... expression

    Args:
        table_name: Name of the table
        old_column: The old name of the column
        new_column: The new name of the column
        exists: Whether to add the `IF EXISTS` clause
        dialect: The dialect to parse the table/column.

    Returns:
        Alter table expression
    """
    table = to_table(table_name, dialect=dialect)
    old_column = to_column(old_column_name, dialect=dialect)
    new_column = to_column(new_column_name, dialect=dialect)
    return Alter(
        this=table,
        kind="TABLE",
        actions=[
            RenameColumn(this=old_column, to=new_column, exists=exists),
        ],
    )


def convert(value: t.Any, copy: bool = False) -> Expression:
    """Convert a python value into an expression object.

    Raises an error if a conversion is not possible.

    Args:
        value: A python object.
        copy: Whether to copy `value` (only applies to Expressions and collections).

    Returns:
        The equivalent expression object.
    """
    if isinstance(value, Expression):
        return maybe_copy(value, copy)
    if isinstance(value, str):
        return Literal.string(value)
    if isinstance(value, bool):
        return Boolean(this=value)
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return null()
    if isinstance(value, numbers.Number):
        return Literal.number(value)
    if isinstance(value, bytes):
        return HexString(this=value.hex())
    if isinstance(value, datetime.datetime):
        datetime_literal = Literal.string(value.isoformat(sep=" "))

        tz = None
        if value.tzinfo:
            # this works for zoneinfo.ZoneInfo, pytz.timezone and datetime.datetime.utc to return IANA timezone names like "America/Los_Angeles"
            # instead of abbreviations like "PDT". This is for consistency with other timezone handling functions in SQLGlot
            tz = Literal.string(str(value.tzinfo))

        return TimeStrToTime(this=datetime_literal, zone=tz)
    if isinstance(value, datetime.date):
        date_literal = Literal.string(value.strftime("%Y-%m-%d"))
        return DateStrToDate(this=date_literal)
    if isinstance(value, datetime.time):
        time_literal = Literal.string(value.isoformat())
        return TsOrDsToTime(this=time_literal)
    if isinstance(value, tuple):
        if hasattr(value, "_fields"):
            return Struct(
                expressions=[
                    PropertyEQ(
                        this=to_identifier(k), expression=convert(getattr(value, k), copy=copy)
                    )
                    for k in value._fields
                ]
            )
        return Tuple(expressions=[convert(v, copy=copy) for v in value])
    if isinstance(value, list):
        return Array(expressions=[convert(v, copy=copy) for v in value])
    if isinstance(value, dict):
        return Map(
            keys=Array(expressions=[convert(k, copy=copy) for k in value]),
            values=Array(expressions=[convert(v, copy=copy) for v in value.values()]),
        )
    if hasattr(value, "__dict__"):
        return Struct(
            expressions=[
                PropertyEQ(this=to_identifier(k), expression=convert(v, copy=copy))
                for k, v in value.__dict__.items()
            ]
        )
    raise ValueError(f"Cannot convert {value}")


def replace_children(expression: Expression, fun: t.Callable, *args, **kwargs) -> None:
    """
    Replace children of an expression with the result of a lambda fun(child) -> exp.
    """
    for k, v in tuple(expression.args.items()):
        is_list_arg = type(v) is list

        child_nodes = v if is_list_arg else [v]
        new_child_nodes = []

        for cn in child_nodes:
            if isinstance(cn, Expression):
                for child_node in ensure_collection(fun(cn, *args, **kwargs)):
                    new_child_nodes.append(child_node)
            else:
                new_child_nodes.append(cn)

        expression.set(k, new_child_nodes if is_list_arg else seq_get(new_child_nodes, 0))


def replace_tree(
    expression: Expression,
    fun: t.Callable,
    prune: t.Optional[t.Callable[[Expression], bool]] = None,
) -> Expression:
    """
    Replace an entire tree with the result of function calls on each node.

    This will be traversed in reverse dfs, so leaves first.
    If new nodes are created as a result of function calls, they will also be traversed.
    """
    stack = list(expression.dfs(prune=prune))

    while stack:
        node = stack.pop()
        new_node = fun(node)

        if new_node is not node:
            node.replace(new_node)

            if isinstance(new_node, Expression):
                stack.append(new_node)

    return new_node


def find_tables(expression: Expression) -> t.Set[Table]:
    """
    Find all tables referenced in a query.

    Args:
        expressions: The query to find the tables in.

    Returns:
        A set of all the tables.
    """
    from sqlglot.optimizer.scope import traverse_scope

    return {
        table
        for scope in traverse_scope(expression)
        for table in scope.tables
        if table.name and table.name not in scope.cte_sources
    }


def column_table_names(expression: Expression, exclude: str = "") -> t.Set[str]:
    """
    Return all table names referenced through columns in an expression.

    Example:
        >>> import sqlglot
        >>> sorted(column_table_names(sqlglot.parse_one("a.b AND c.d AND c.e")))
        ['a', 'c']

    Args:
        expression: expression to find table names.
        exclude: a table name to exclude

    Returns:
        A list of unique names.
    """
    return {
        table
        for table in (column.table for column in expression.find_all(Column))
        if table and table != exclude
    }


def table_name(table: Table | str, dialect: DialectType = None, identify: bool = False) -> str:
    """Get the full name of a table as a string.

    Args:
        table: Table expression node or string.
        dialect: The dialect to generate the table name for.
        identify: Determines when an identifier should be quoted. Possible values are:
            False (default): Never quote, except in cases where it's mandatory by the dialect.
            True: Always quote.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> table_name(parse_one("select * from a.b.c").find(exp.Table))
        'a.b.c'

    Returns:
        The table name.
    """

    table = maybe_parse(table, into=Table, dialect=dialect)

    if not table:
        raise ValueError(f"Cannot parse {table}")

    return ".".join(
        (
            part.sql(dialect=dialect, identify=True, copy=False, comments=False)
            if identify or not SAFE_IDENTIFIER_RE.match(part.name)
            else part.name
        )
        for part in table.parts
    )


def normalize_table_name(table: str | Table, dialect: DialectType = None, copy: bool = True) -> str:
    """Returns a case normalized table name without quotes.

    Args:
        table: the table to normalize
        dialect: the dialect to use for normalization rules
        copy: whether to copy the expression.

    Examples:
        >>> normalize_table_name("`A-B`.c", dialect="bigquery")
        'A-B.c'
    """
    from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

    return ".".join(
        p.name
        for p in normalize_identifiers(
            to_table(table, dialect=dialect, copy=copy), dialect=dialect
        ).parts
    )


def replace_tables(
    expression: E, mapping: t.Dict[str, str], dialect: DialectType = None, copy: bool = True
) -> E:
    """Replace all tables in expression according to the mapping.

    Args:
        expression: expression node to be transformed and replaced.
        mapping: mapping of table names.
        dialect: the dialect of the mapping table
        copy: whether to copy the expression.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> replace_tables(parse_one("select * from a.b"), {"a.b": "c"}).sql()
        'SELECT * FROM c /* a.b */'

    Returns:
        The mapped expression.
    """

    mapping = {normalize_table_name(k, dialect=dialect): v for k, v in mapping.items()}

    def _replace_tables(node: Expression) -> Expression:
        if isinstance(node, Table) and node.meta.get("replace") is not False:
            original = normalize_table_name(node, dialect=dialect)
            new_name = mapping.get(original)

            if new_name:
                table = to_table(
                    new_name,
                    **{k: v for k, v in node.args.items() if k not in TABLE_PARTS},
                    dialect=dialect,
                )
                table.add_comments([original])
                return table
        return node

    return expression.transform(_replace_tables, copy=copy)  # type: ignore


def replace_placeholders(expression: Expression, *args, **kwargs) -> Expression:
    """Replace placeholders in an expression.

    Args:
        expression: expression node to be transformed and replaced.
        args: positional names that will substitute unnamed placeholders in the given order.
        kwargs: keyword arguments that will substitute named placeholders.

    Examples:
        >>> from sqlglot import exp, parse_one
        >>> replace_placeholders(
        ...     parse_one("select * from :tbl where ? = ?"),
        ...     exp.to_identifier("str_col"), "b", tbl=exp.to_identifier("foo")
        ... ).sql()
        "SELECT * FROM foo WHERE str_col = 'b'"

    Returns:
        The mapped expression.
    """

    def _replace_placeholders(node: Expression, args, **kwargs) -> Expression:
        if isinstance(node, Placeholder):
            if node.this:
                new_name = kwargs.get(node.this)
                if new_name is not None:
                    return convert(new_name)
            else:
                try:
                    return convert(next(args))
                except StopIteration:
                    pass
        return node

    return expression.transform(_replace_placeholders, iter(args), **kwargs)


def expand(
    expression: Expression,
    sources: t.Dict[str, Query | t.Callable[[], Query]],
    dialect: DialectType = None,
    copy: bool = True,
) -> Expression:
    """Transforms an expression by expanding all referenced sources into subqueries.

    Examples:
        >>> from sqlglot import parse_one
        >>> expand(parse_one("select * from x AS z"), {"x": parse_one("select * from y")}).sql()
        'SELECT * FROM (SELECT * FROM y) AS z /* source: x */'

        >>> expand(parse_one("select * from x AS z"), {"x": parse_one("select * from y"), "y": parse_one("select * from z")}).sql()
        'SELECT * FROM (SELECT * FROM (SELECT * FROM z) AS y /* source: y */) AS z /* source: x */'

    Args:
        expression: The expression to expand.
        sources: A dict of name to query or a callable that provides a query on demand.
        dialect: The dialect of the sources dict or the callable.
        copy: Whether to copy the expression during transformation. Defaults to True.

    Returns:
        The transformed expression.
    """
    normalized_sources = {normalize_table_name(k, dialect=dialect): v for k, v in sources.items()}

    def _expand(node: Expression):
        if isinstance(node, Table):
            name = normalize_table_name(node, dialect=dialect)
            source = normalized_sources.get(name)

            if source:
                # Create a subquery with the same alias (or table name if no alias)
                parsed_source = source() if callable(source) else source
                subquery = parsed_source.subquery(node.alias or name)
                subquery.comments = [f"source: {name}"]

                # Continue expanding within the subquery
                return subquery.transform(_expand, copy=False)

        return node

    return expression.transform(_expand, copy=copy)


def func(name: str, *args, copy: bool = True, dialect: DialectType = None, **kwargs) -> Func:
    """
    Returns a Func expression.

    Examples:
        >>> func("abs", 5).sql()
        'ABS(5)'

        >>> func("cast", this=5, to=DataType.build("DOUBLE")).sql()
        'CAST(5 AS DOUBLE)'

    Args:
        name: the name of the function to build.
        args: the args used to instantiate the function of interest.
        copy: whether to copy the argument expressions.
        dialect: the source dialect.
        kwargs: the kwargs used to instantiate the function of interest.

    Note:
        The arguments `args` and `kwargs` are mutually exclusive.

    Returns:
        An instance of the function of interest, or an anonymous function, if `name` doesn't
        correspond to an existing `sqlglot.expressions.Func` class.
    """
    if args and kwargs:
        raise ValueError("Can't use both args and kwargs to instantiate a function.")

    from sqlglot.dialects.dialect import Dialect

    dialect = Dialect.get_or_raise(dialect)

    converted: t.List[Expression] = [maybe_parse(arg, dialect=dialect, copy=copy) for arg in args]
    kwargs = {key: maybe_parse(value, dialect=dialect, copy=copy) for key, value in kwargs.items()}

    constructor = dialect.parser_class.FUNCTIONS.get(name.upper())
    if constructor:
        if converted:
            code = getattr(constructor, "__code__", None)
            if code is not None and "dialect" in code.co_varnames:
                function = constructor(converted, dialect=dialect)
            else:
                function = constructor(converted)
        elif constructor.__name__ == "from_arg_list":
            function = constructor.__self__(**kwargs)  # type: ignore
        else:
            constructor = FUNCTION_BY_NAME.get(name.upper())
            if constructor:
                function = constructor(**kwargs)
            else:
                raise ValueError(
                    f"Unable to convert '{name}' into a Func. Either manually construct "
                    "the Func expression of interest or parse the function call."
                )
    else:
        kwargs = kwargs or {"expressions": converted}
        function = Anonymous(this=name, **kwargs)

    for error_message in function.error_messages(converted):
        raise ValueError(error_message)

    return function


def case(
    expression: t.Optional[ExpOrStr] = None,
    **opts,
) -> Case:
    """
    Initialize a CASE statement.

    Example:
        case().when("a = 1", "foo").else_("bar")

    Args:
        expression: Optionally, the input expression (not all dialects support this)
        **opts: Extra keyword arguments for parsing `expression`
    """
    if expression is not None:
        this = maybe_parse(expression, **opts)
    else:
        this = None
    return Case(this=this, ifs=[])


def array(
    *expressions: ExpOrStr, copy: bool = True, dialect: DialectType = None, **kwargs
) -> Array:
    """
    Returns an array.

    Examples:
        >>> array(1, 'x').sql()
        'ARRAY(1, x)'

    Args:
        expressions: the expressions to add to the array.
        copy: whether to copy the argument expressions.
        dialect: the source dialect.
        kwargs: the kwargs used to instantiate the function of interest.

    Returns:
        An array expression.
    """
    return Array(
        expressions=[
            maybe_parse(expression, copy=copy, dialect=dialect, **kwargs)
            for expression in expressions
        ]
    )


def tuple_(
    *expressions: ExpOrStr, copy: bool = True, dialect: DialectType = None, **kwargs
) -> Tuple:
    """
    Returns an tuple.

    Examples:
        >>> tuple_(1, 'x').sql()
        '(1, x)'

    Args:
        expressions: the expressions to add to the tuple.
        copy: whether to copy the argument expressions.
        dialect: the source dialect.
        kwargs: the kwargs used to instantiate the function of interest.

    Returns:
        A tuple expression.
    """
    return Tuple(
        expressions=[
            maybe_parse(expression, copy=copy, dialect=dialect, **kwargs)
            for expression in expressions
        ]
    )


def true() -> Boolean:
    """
    Returns a true Boolean expression.
    """
    return Boolean(this=True)


def false() -> Boolean:
    """
    Returns a false Boolean expression.
    """
    return Boolean(this=False)


def null() -> Null:
    """
    Returns a Null expression.
    """
    return Null()


NONNULL_CONSTANTS = (
    Literal,
    Boolean,
)

CONSTANTS = (
    Literal,
    Boolean,
    Null,
)


def apply_index_offset(
    this: Expression,
    expressions: t.List[E],
    offset: int,
    dialect: DialectType = None,
) -> t.List[E]:
    if not offset or len(expressions) != 1:
        return expressions

    expression = expressions[0]

    from sqlglot.optimizer.annotate_types import annotate_types
    from sqlglot.optimizer.simplify import simplify

    if not this.type:
        annotate_types(this, dialect=dialect)

    if t.cast(DataType, this.type).this not in (
        DataType.Type.UNKNOWN,
        DataType.Type.ARRAY,
    ):
        return expressions

    if not expression.type:
        annotate_types(expression, dialect=dialect)

    if t.cast(DataType, expression.type).this in DataType.INTEGER_TYPES:
        logger.info("Applying array index offset (%s)", offset)
        expression = simplify(expression + offset)
        return [expression]

    return expressions
