# TODO: Consider re-writting without using DataFrames. Not worth the performance gains likely
# TODO: Digimon with mode changes between the same generation are not properly recorded (i.e. Ceresmon/Cersmon Medium, Bacchusmon/Bacchusmon DM)
# TODO: Decrypt live save, check for progress of live dex and adjust needed digimon.
#   - Force reading as UTF-8 and swallowing the UnicodeEncodeError gets a decent result if we're just tracking the Digimon the player has obtained

import polars as pl
import pprint as pp
import re
import binascii

df_data_columns = {
    "0": "id",
    "2": "name",
    "4": "generation",
}

df_chart_columns = {
    "1": "from_digimon_id",
    "3": "to_digimon_id",
    "5": "digivolution_type",
}

fusion_digimon = {"char_DINOBEEMON":23, "char_OMEGAMON":88, "char_SUSANOOMON":104, "char_CHAOSMONVALDURARM":118, "char_EXAMON":215, "char_MILLENNIUMON":230, "char_PAILDRAMON":408, "char_GRACENOVAMON":604, "char_SILPHYMON":720, "char_SHAKKOUMON":723, "char_MASTEMON":748, "char_ALPHAMON_OURYUKEN":766, "char_CHAOSMON":772, "char_SKULLBALUCHIMON_TITAMON":915, "char_ENBARRMON_CRANIAMON":494, "char_OMEGAMON_ZWART":757}

digi_ids_mode_change = []
df_digi_chart = pl.DataFrame()
df_digi_count = pl.DataFrame()
df_digi_tracker = pl.DataFrame()

def _print_df(df: pl.DataFrame):
    print(''.join([f'{col:<60}' for col in df.columns]))
    for row in df.rows():
        line = ''.join([f'{x:<60}' for x in row])
        print(line)
    print('')

def _cleanup_raw_columns(df: pl.DataFrame):
    df.columns = [re.sub(r".* (\d+)", r"\1", col) for col in df.columns]
    return df

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
    # _print_df(df_digi)

    df_digi_count = pl.concat([df_digi_count, df_digi]).group_by("id").agg(pl.col("count").sum())
    # print("Result")
    # _print_df(df_digi_count)

def update_digi_count(df_digi: pl.DataFrame, digi_ids_previous: list[str]=[]):
    # Update digi count
    add_to_digi_count(df_digi)

    digi_ids = df_digi["id"].unique().to_list()
    df_digi_next = df_digi.join(df_digi_chart, left_on="id", right_on="to_digimon_id")\
                          .join(df_digi_count, left_on="from_digimon_id", right_on="id")\
                          .filter(~pl.col("from_digimon_id").is_in(digi_ids_mode_change) & ~pl.col("from_digimon_id").is_in(digi_ids_previous))\
                          .with_columns(pl.col("id").is_in(fusion_digimon.values()).alias("is_fusion"))
    
    df_fusion = df_digi_next.filter(pl.col("is_fusion"))
    df_non_fusion = df_digi_next.filter(~pl.col("is_fusion")).group_by("origin_digimon_id")\
                                .agg(
                                    # When choosing which pre-digivolution to choose, choose the one that's appeared most often already
                                    # i.e. "If you already have to farm a digimon a decent bit, then you already have a good farm for them, farm a few more" typeshit typeshit
                                    pl.col("from_digimon_id").filter(pl.col("count") == pl.col("count").max()).first().alias("from_digimon_id"),
                                )
    
    df_digi_next = pl.concat([df_non_fusion.select(["from_digimon_id", "origin_digimon_id"]), df_fusion.select(["from_digimon_id", "origin_digimon_id"])])\
                     .rename({"from_digimon_id":"id"})\

    if len(df_digi_next) == 0:
        return

    update_digi_count(df_digi_next, digi_ids)
    
def main():
    global df_digi_chart, df_digi_count, digi_ids_mode_change

    with open("data/decrypted_save.bin", encoding='utf-8', errors="ignore") as file:
        content = file.read()
        matches = re.findall(r"[ \w-]+", content)
        
        for match in matches:
            try:
                print(match)
            except UnicodeEncodeError:
                pass

    df_digi_data = _cleanup_raw_columns(pl.read_csv("data/000_digimon_status_data.csv")).select(df_data_columns.keys()).rename(df_data_columns)

    df_digi_chart = _cleanup_raw_columns(pl.read_csv("data/001_evolution_to.csv")).select(df_chart_columns.keys()).rename(df_chart_columns)
    digi_ids_mode_change = df_digi_chart.filter(pl.col("digivolution_type") == 2)["to_digimon_id"].to_list()

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

    _print_df(df_digi_count.select(["id", "name", "count", "generation"]))
    _print_df(df_digi_tracker.select(["origin_digimon_id", "id"]).sort("origin_digimon_id"))

if __name__ == "__main__":
    main()
