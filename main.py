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

df_digi_chart = pl.DataFrame()
df_digi_count = pl.DataFrame()

def print_df(df: pl.DataFrame):
    print(''.join([f'{col:<60}' for col in df.columns]))
    for row in df.rows():
        line = ''.join([f'{x:<60}' for x in row])
        print(line)
    print('')

def update_digi_count(digi_names: list[str], digi_names_previous: list[str]=[]):
    global df_digi_count 

    # Update digi count
    df = pl.DataFrame({"name": digi_names, "digi_count": [1]*len(digi_names)})
    df_digi_count = pl.concat([df_digi_count, df]).group_by("name").agg(pl.col("digi_count").sum())

    digi_next_gen = df_digi_chart.filter(pl.col("to_name").is_in(digi_names) & ~pl.col("from_name").is_in(digi_names) & ~pl.col("from_name").is_in(digi_names_previous))

    # TODO: Change the aggregation so that it pulls the first Digimon that also appears in df_digi_count, otherwise just takes the first of the agg group
    digi_next_gen = digi_next_gen.group_by("target_digimon_id").agg(pl.first("from_name"))
    if len(digi_next_gen) == 0:
        return
    
    new_digi_names = digi_next_gen["from_name"].to_list()
    update_digi_count(new_digi_names, digi_names)
    
def main():
    global df_digi_chart, df_digi_count

    df_digi_data = pl.read_csv("data/digimon_status_data.csv")
    df_digi_data = df_digi_data.select(df_data_columns.keys()).rename(df_data_columns)

    df_digi_chart = pl.read_csv("data/digivolution_chart.csv")
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="source_digimon_id", right_on="index", how="inner").rename({"name": "from_name", "generation": "from_generation"})
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="target_digimon_id", right_on="index", how="inner").rename({"name": "to_name", "generation": "to_generation"})

    generation_list = pl.concat([df_digi_chart["from_generation"], df_digi_chart["to_generation"]]).unique().to_list()
    generation_list.sort()

    # Handle special case of gen 1 digimon (in-training-I)
    generation_list.remove(1)
    digi_names = df_digi_chart.filter(pl.col("from_generation") == 1)["from_name"].unique().to_list()
    df = pl.DataFrame({"name": digi_names, "digi_count": [1]*len(digi_names)})
    df_digi_count = pl.concat([df_digi_count, df]).group_by("name").agg(pl.col("digi_count").sum())

    # TODO: Incorrect, counts Jupitermon x5. Jupitermon only evolves in to Jupitermon Wrath Mode and should be counted only x2.
    for gen in generation_list:
        _df = df_digi_chart.filter(pl.col("to_generation") == gen)
        print(gen)
        print_df(_df)

        target_digi_names = _df["to_name"].unique().to_list()
        print(target_digi_names)
        update_digi_count(target_digi_names)
        
        print_df(df_digi_count)
        print("#" * 50)

    df_digi_count.write_csv("out.csv")

if __name__ == "__main__":
    main()
