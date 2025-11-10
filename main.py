import polars as pl
import pprint as pp

df_data_columns = {
    "col_0": "index",
    "col_2": "name",
    "col_4": "generation",
}

df_data_columns = {
    "col_0": "index",
    "col_2": "name",
    "col_4": "generation",
}

digi_names_in_training_I = [
    "Kuramon",
    "Choromon",
    "Dodomon",
    "Pabumon",
    "Punimon",
    "Botamon",
    "Poyomon",
]

digi_names_in_training_II = [
    "Kapurimon",
    "Koromon",
    "Tanemon",
    "Tsunomon",
    "Tsumemon",
    "Tokomon",
    "Dorimon",
    "Nyaromon",
    "Pagumon",
    "Yokomon",
    "Bukamon",
    "Motimon",
    "Wanyamon",
]

digi_names_rookie = [
    "Agumon",
    "Kudamon",
    "Gomamon",
    "Coronamon",
    "Zubamon",
    "Solarmon",
    "Terriermon",
    "Tentomon",
    "ToyAgumon",
    "Tapirmon",
    "Hyokomon",
    "Biyomon",
    "Falcomon",
    "Salamon",
    "Bearmon",
    "Penmon",
    "Monodramon",
    "Ryudamon",
    "Lucemon",
    "Elecmon",
    "Gaomon",
    "Crabmon",
    "Gabumon",
    "Kamemon",
    "Kokuwamon",
    "Gotsumon",
    "Kotemon",
    "Shoutmon",
    "Dracomon",
    "Dorumon",
    "Patamon",
    "Huckmon",
    "Palmon",
    "Floramon",
    "Muchomon",
    "Lalamon",
    "Lunamon",
    "Renamon",
    "Lopmon",
    "Impmon",
    "Otamamon",
    "Gazimon",
    "Gizamon",
    "Guilmon",
    "Goblimon",
    "Shamamon",
    "Syakomon",
    "SnowGoblimon",
    "Chuumon",
    "Dracmon",
    "Hagurumon",
    "DemiDevimon",
    "FunBeemon",
    "Betamon",
    "Mushroomon",
    "Armadillomon",
    "Veemon",
    "Hawkmon",
    "Wormmon",
    "Keramon",
]

target_columns = [
    "col_4",
    "col_5",
    "col_6",
    "col_51",
    "col_62",
    "col_74",
    "col_77",
    "col_80",
    "col_83",
    "col_84",
    "col_86",
    "col_87",
    "col_89",
    "col_90",
    "col_92",
    "col_93",
    "col_95",
    "col_96",
    "col_98",
    "col_99",
    "col_101",
    "col_102",
    "col_104",
    "col_105",
    "col_107",
    "col_110",
    "col_120",
    "col_123",
    "col_134",
]

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

    df = pl.DataFrame({"name": digi_names, "digi_count": [1]*len(digi_names)})
    df_digi_count = pl.concat([df_digi_count, df]).group_by("name").agg(pl.col("digi_count").sum())

    digi_next_gen = df_digi_chart.filter(pl.col("to_name").is_in(digi_names) & ~pl.col("from_name").is_in(digi_names) & ~pl.col("from_name").is_in(digi_names_previous))
    digi_next_gen = digi_next_gen.select(["Target_Digimon_ID", "from_name"])

    # TODO: Change the aggregation so that it pulls the first Digimon that also appears in df_digi_count, otherwise just takes the first of the agg group
    digi_next_gen = digi_next_gen.group_by("Target_Digimon_ID").agg(pl.first("from_name"))
    if len(digi_next_gen) == 0:
        return
    
    new_digi_names = digi_next_gen["from_name"].to_list()
    update_digi_count(new_digi_names, digi_names)
    
def main():
    global df_digi_chart

    df_digi_data = pl.read_csv("data/digimon_status_data.csv")
    df_digi_data = df_digi_data.select(df_data_columns.keys()).rename(df_data_columns)

    df_digi_chart = pl.read_csv("data/digivolution_chart.csv")
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="Source_Digimon_ID", right_on="index", how="inner").rename({"name": "from_name", "generation": "from_generation"})
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="Target_Digimon_ID", right_on="index", how="inner").rename({"name": "to_name", "generation": "to_generation"})

    generation_list = pl.concat([df_digi_chart["from_generation"], df_digi_chart["to_generation"]]).unique().to_list()
    generation_list.sort(reverse=True)
    
    # TODO: Incorrect, counts Jupitermon x5. Jupitermon only evolves in to Jupitermon Wrath Mode and should be counted only x2.
    for gen in generation_list:
        _df = df_digi_chart.filter(pl.col("to_generation") == gen)
        update_digi_count(_df["to_name"].to_list())

    df_digi_count.write_csv("out.csv")

if __name__ == "__main__":
    main()
