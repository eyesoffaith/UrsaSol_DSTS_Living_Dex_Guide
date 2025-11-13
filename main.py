import polars as pl
import pprint as pp

df_data_columns = {
    "col_0": "id",
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
df_digi_tracker = pl.DataFrame(schema={"id":pl.Int64, "origin_digimon_id":pl.Int64})

def print_df(df: pl.DataFrame):
    print(''.join([f'{col:<60}' for col in df.columns]))
    for row in df.rows():
        line = ''.join([f'{x:<60}' for x in row])
        print(line)
    print('')

# TODO: Finish this method
# For some reason it's not recording every record for each digimon (i.e. EX-Tyranomon (id #1) should have 5 records (one for itself and it's four pre-digivolutions))
def add_to_digi_tracker(df_digi: pl.DataFrame):
    global df_digi_tracker

    df_digi_tracker = pl.concat([df_digi_tracker, df_digi])

def add_to_digi_count(df_digi: pl.DataFrame):
    global df_digi_count
    add_to_digi_tracker(df_digi)

    df_digi = df_digi.group_by("id").agg(pl.len().alias("count"))
    # print("Adding to digi")
    # print_df(df_digi)

    df_digi_count = pl.concat([df_digi_count, df_digi]).group_by("id").agg(pl.col("count").sum())
    # print("Result")
    # print_df(df_digi_count)

def update_digi_count(df_digi: pl.DataFrame, digi_ids_previous: list[str]=[]):
    # Update digi count
    add_to_digi_count(df_digi)

    digi_ids = df_digi["id"].unique().to_list()

    # The filter prevents looping into other digis in the same generation (i.e. Omnimon to Omnimon Zwart) or to a digimon from the previous step (i.e. Ceresmon to Ceresmon Medium)
    # The "count" here is the count of the "from_digimon" so we can choose the pre-digivolution that has the highest "count"
    df_digi_next = df_digi.join(df_digi_chart, left_on="id", right_on="to_digimon_id")\
                          .join(df_digi_count, left_on="from_digimon_id", right_on="id")\
                          .filter(~pl.col("from_digimon_id").is_in(digi_ids) & ~pl.col("from_digimon_id").is_in(digi_ids_previous))\
                          .group_by("origin_digimon_id")\
                          .agg(
                                # When choosing which pre-digivolution to choose, choose the one that's appeared most often already
                                # i.e. "If you already have to farm a digimon a decent bit, then you already have a good farm for them, farm a few more" typeshit typeshit
                                pl.col("from_digimon_id").filter(pl.col("count") == pl.col("count").max()).first().alias("from_digimon_id"),
                          )\
                          .rename({"from_digimon_id":"id"})\
                          .select(["id", "origin_digimon_id"])
    
    if len(df_digi_next) == 0:
        return

    update_digi_count(df_digi_next, digi_ids)
    
def main():
    global df_digi_chart, df_digi_count

    df_digi_data = pl.read_csv("data/digimon_status_data.csv")
    df_digi_data = df_digi_data.select(df_data_columns.keys()).rename(df_data_columns)

    df_digi_chart = pl.read_csv("data/digivolution_chart.csv")
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="from_digimon_id", right_on="id", how="inner").rename({"name": "from_name", "generation": "from_generation"})
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="to_digimon_id", right_on="id", how="inner").rename({"name": "to_name", "generation": "to_generation"})

    generation_list = pl.concat([df_digi_chart["from_generation"], df_digi_chart["to_generation"]]).unique().to_list()
    generation_list.sort()

    # Handle initial case of gen 1 digimon (In-Training I)
    generation_list.remove(1)
    digi_gen_1 = df_digi_chart.filter(pl.col("from_generation") == 1)\
                              .select("from_digimon_id").unique()\
                              .with_columns(pl.col("from_digimon_id").alias("origin_digimon_id"))\
                              .rename({"from_digimon_id":"id"})
    add_to_digi_count(digi_gen_1)

    # TODO: Potentially incorrect. Inconsistent values between runs???
    for gen in generation_list:
        digi_for_gen = df_digi_chart.filter(pl.col("to_generation") == gen)\
                                    .select("to_digimon_id").unique()\
                                    .with_columns(pl.col("to_digimon_id").alias("origin_digimon_id"))\
                                    .rename({"to_digimon_id":"id"})
        update_digi_count(digi_for_gen)
        
    df_digi_count = df_digi_count.join(df_digi_data, on="id", how="left")
    df_digi_count = df_digi_count.sort(["generation", "count"], descending=[False, True])
    
    df_digi_count.select(["id", "name", "count"]).write_csv("out.csv")

    print(df_digi_count["count"].sum())
    print_df(df_digi_count.select(["id", "name", "count", "generation"]))
    print_df(df_digi_tracker.select(["origin_digimon_id", "id"]).sort("origin_digimon_id"))

if __name__ == "__main__":
    main()
