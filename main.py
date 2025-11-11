import polars as pl
import pprint as pp

df_data_columns = {
    "col_0": "index",
    "col_2": "name",
    "col_4": "generation",
}

df_chart_columns = {
    "col_0": "index",
    "col_2": "name",
    "col_4": "generation",
}

df_chart_column_order = ["from_digimon_id", "to_digimon_id", "from_name", "to_name", "from_generation", "to_generation"]

df_digi_chart = pl.DataFrame()
df_digi_count = pl.DataFrame()

def print_df(df: pl.DataFrame):
    print(''.join([f'{col:<60}' for col in df.columns]))
    for row in df.rows():
        line = ''.join([f'{x:<60}' for x in row])
        print(line)
    print('')

def add_to_digi_count(df_digi: pl.DataFrame):
    global df_digi_count

    df_digi = df_digi.group_by("name").agg(
        pl.col("digi_count").sum(),
        pl.col("generation").first()
    )
    # print("Adding to digi")
    # print_df(df_digi)

    df_digi_count = pl.concat([df_digi_count, df_digi]).group_by("name").agg(
        pl.col("digi_count").sum(),
        pl.col("generation").first()
    )
    # print("Result")
    # print_df(df_digi_count)

def update_digi_count(df_digi: pl.DataFrame, digi_names_previous: list[str]=[]):
    # Update digi count
    add_to_digi_count(df_digi)

    digi_names = df_digi["name"].unique().to_list()
    digi_next_gen = df_digi_chart.filter(pl.col("to_name").is_in(digi_names) & ~pl.col("from_name").is_in(digi_names) & ~pl.col("from_name").is_in(digi_names_previous))\
                                 .join(df_digi_count, left_on="from_name", right_on="name")\
                                 .group_by("to_digimon_id")\
                                 .agg(
                                    # When choosing which pre-digivolution to choose, choose the one that's appeared most often already
                                    # i.e. "If you already have to farm a digimon a decent bit, then you already have a good farm for them, farm a few more" typeshit typeshit
                                    pl.col("from_name").filter(pl.col("digi_count") == pl.col("digi_count").max()).first().alias("from_name"),
                                    pl.col("to_name").filter(pl.col("digi_count") == pl.col("digi_count").max()).first().alias("to_name"),
                                    pl.col("from_generation").filter(pl.col("digi_count") == pl.col("digi_count").max()).first().alias("from_generation"),
                                    pl.col("digi_count").max()
                                 ).sort("to_digimon_id")

    if len(digi_next_gen) == 0:
        return

    df_digi_next = df_digi.join(digi_next_gen, left_on="name", right_on="to_name")\
                          .select(["from_name", "from_generation", "digi_count"])\
                          .rename({"from_name": "name", "from_generation":"generation"})
    update_digi_count(df_digi_next, digi_names)
    
def main():
    global df_digi_chart, df_digi_count

    df_digi_data = pl.read_csv("data/digimon_status_data.csv")
    df_digi_data = df_digi_data.select(df_data_columns.keys()).rename(df_data_columns)

    df_digi_chart = pl.read_csv("data/digivolution_chart.csv")
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="from_digimon_id", right_on="index", how="inner").rename({"name": "from_name", "generation": "from_generation"})
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="to_digimon_id", right_on="index", how="inner").rename({"name": "to_name", "generation": "to_generation"})

    generation_list = pl.concat([df_digi_chart["from_generation"], df_digi_chart["to_generation"]]).unique().to_list()
    generation_list.sort()

    # Handle initial case of gen 1 digimon (In-Training I)
    generation_list.remove(1)
    digi_gen_1 = df_digi_chart.filter(pl.col("from_generation") == 1)\
                              .select(["from_name", "from_generation"])\
                              .unique()\
                              .with_columns(pl.lit(1).alias("digi_count"))\
                              .rename({"from_name":"name", "from_generation":"generation"})
    add_to_digi_count(digi_gen_1)

    # TODO: Potentially incorrect. Inconsistent values every run???
    for gen in generation_list:
        digi_for_gen = df_digi_chart.filter(pl.col("to_generation") == gen)\
                                    .select(["to_name", "to_generation"])\
                                    .unique()\
                                    .with_columns(pl.lit(1).alias("digi_count"))\
                                    .rename({"to_name":"name", "to_generation":"generation"})
        update_digi_count(digi_for_gen)
        
    df_digi_count = df_digi_count.sort(["generation", "digi_count"], descending=[False, True])
    
    df_digi_count.select(["name", "digi_count"]).write_csv("out.csv")

    print_df(df_digi_count)

if __name__ == "__main__":
    main()
