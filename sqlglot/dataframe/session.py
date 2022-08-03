from sqlglot.dataframe.dataframe_reader import DataFrameReader


class SparkSession:
    @property
    def read(self) -> "DataFrameReader":
        return DataFrameReader(self)


if __name__ == '__main__':
    from sqlglot.dataframe import functions as F
    spark = SparkSession()
    # df1 = spark.read.table("dse.table_name1")
    # df1 = df1.where(F.col("test") != F.lit(1))
    #
    # df2 = spark.read.table("dse.table_name2")
    # df2 = (
    #     df2.alias("df2")
    #     .groupBy("test")
    #     .agg(F.count_distinct("account").alias("the_col"))
    #     .join(
    #         df1,
    #         on=["test"],
    #         how="left_outer"
    #     )
    #     .select("df2.*")
    # )
    # print(df2.sql())
    #
    # df = (
    #     spark.read.table("dse.table") # FROM - 1
    #     .alias("df") # NO-OP
    #     .select("col1", "col2") # SELECT - 5
    #     .groupBy("col1") # GROUP BY - 3
    #     .agg(F.count_distinct("col2").alias("num_col2")) # SELECT - 5
    #     .where(F.col("num_col2") > F.lit(1)) # WHERE - 2
    # )
    # print(df.sql())
    # df = spark.read.table("dse.table").alias("df")  # FROM - 1
    # df = df.select("col1", "col2")  # SELECT - 5
    # df = df.groupBy("col1")  # GROUP BY - 3
    # df = df.agg(F.count_distinct("col2").alias("num_col2"))  # SELECT - 5
    # df = df.where(F.col("num_col2") > F.lit(1))  # WHERE - 2
    # print(df.sql())
    #
    # df_first_play = (
    #     spark.read.table("dse.game_session_f")
    #     .alias("gsf")
    #     .where(F.col("play_secs") > F.lit(0))
    #     .groupBy(F.col("account_id"))
    #     .agg(F.min(F.col("play_start_region_date")).alias("first_play_region_date"))
    # )
    #
    # df_first_play_2 = (
    #     df_first_play
    #     .select(
    #         df_first_play.account_id,
    #         df_first_play.first_play_region_date.alias("first_play_blah")
    #     )
    # )
    #
    # df_first_subscribe = (
    #     spark.read.table("dse.subscrn_d")
    #     .select(F.col("account_id"), F.col("signup_region_date"))
    # )
    #
    # df_joined = (
    #     df_first_play.alias("first_play")
    #     .join(
    #         df_first_subscribe.alias("first_sub"),
    #         on=["account_id"],
    #         how="inner"
    #     )
    #     .join(
    #         df_first_play_2,
    #         on=["account_id"],
    #         how="inner"
    #     )
    #     .select(
    #         df_first_play.account_id,
    #         F.col("first_play.first_play_region_date"),
    #         F.col("first_sub.signup_region_date"),
    #         df_first_subscribe.signup_region_date,
    #         df_first_play_2.first_play_blah
    #     )
    # )

    # print(df_joined.sql())

    # df_base = spark.read.table("dse.base_table")  # 0
    #
    # df_one = (  # 1
    #     df_base
    #     .where(F.col("blah") > F.lit(1))
    # )
    #
    # df_two = (  # 2
    #     df_one
    #     .where(F.col("blah") > F.lit(-1))
    # )
    #
    # df_joined = (
    #     df_base.where(F.col("col2") > F.lit(1000))  # 3
    #     .join(
    #         df_one.where(F.col("col2") > F.lit(2000)),  # 4
    #         on=["col1"],
    #         how="inner"
    #     )
    #     .join(
    #         df_two.where(F.col("col3") < F.lit(-100)), # 5
    #         on=["col1"],
    #         how="inner",
    #     )
    #     .select(
    #         df_base.col1,  # 0
    #         df_one.col2,  # 1
    #         df_two.col2  # 2
    #     )
    #     # 0 -> 1 -> 3
    #     # 0 -> 2
    # )

    # df_base = spark.read.table("dse.ttl_game_d")

    df_base = spark.read.table("dse.ttl_game_d")
    df_second = df_base.where(F.col("game_title_id") == F.lit(81458395))

    df_joined = (
        df_base.where(F.col("game_title_id") == F.lit(81457007)).alias("first_df")
        .join(
            df_second.alias("second_df"),
            on=["game_title_id"],
            how="inner"
        )
        .select(
            df_base.game_desc,
            F.col("first_df.game_desc"),
            df_second.game_desc,
            F.col("second_df.game_desc")
        )
    )

    print(df_joined.sql())

    """
    df1 = spark.read.table("table1") # id=1
    df2 = spark.read.table("table2") # id=2
    df_joined = (
        df1.alias("df1") # id=3
        .join( # id=5
            df2.alias("df2") # id=4
            on=["col1"],
            how="inner"
        )
        .select( # id=6
            df1.col1, <- # I want 3 but I will get 1
            df2.col2, <- # I want 4 but I will get 2
        )
    )
    
    """

"""
df = spark.read.table("dse.table") # FROM - 1
df = df.select("col1", "col2") # SELECT - 5
df = df.group_by("col1") # GROUP BY - 3
df = df.agg(F.count_distinct("col2").alias("num_col2")) # SELECT - 5
df = df.where(F.col("num_col2") > 1) # WHERE - 2

with random1 as (
SELECT
    col1,
    col2
FROM
    dse.table
)
, random2 as (
SELECT
    col1,
    count(distinct col2) as num_col2
FROM
    random1
)
SELECT
    col1
    , num_col2
FROM
    random2
WHERE
    num_col2 > 1


"""