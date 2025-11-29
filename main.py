# TODO: Digimon with mode changes between the same generation are not properly recorded (i.e. Ceresmon/Cersmon Medium, Bacchusmon/Bacchusmon DM)
# TODO: Create UI for save selection and viewing progress

# NOTE: The original generation pulled from digimon_status.csv is overwritten by my own generation value. This helps to standardize the hybrid/armor digivolutions with the rest of the data.

import polars as pl
import pprint as pp
import re
import os
import sys
import time
import glob
import shutil
import binascii
import subprocess

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
import tkinter
import customtkinter

CHOSEN_SAVE_FILE = "0000.bin"
GAME_DIR = r"P:\Program Files (x86)\Steam\steamapps\common\Digimon Story Time Stranger"
ENCRYPTION_KEY = "33393632373736373534353535383833"
FUSION_DIGIMON = {
 "char_DINOBEEMON":23,
 "char_OMEGAMON":88,
 "char_SUSANOOMON":104,
 "char_CHAOSMONVALDURARM":118,
 "char_EXAMON":215,
 "char_MILLENNIUMON":230,
 "char_PAILDRAMON":408,
 "char_GRACENOVAMON":604,
 "char_SILPHYMON":720,
 "char_SHAKKOUMON":723,
 "char_MASTEMON":748,
 "char_ALPHAMON_OURYUKEN":766,
 "char_CHAOSMON":772,
 "char_SKULLBALUCHIMON_TITAMON":915,
 "char_ENBARRMON_CRANIAMON":494,
 "char_OMEGAMON_ZWART":757
}

saves = {}
df_data_columns = {
    "0": "id",
    "2": "name",
    "4": "generation",
    "5": "is_boss"
}
df_chart_columns = {
    "1": "from_digimon_id",
    "3": "to_digimon_id",
    "5": "digivolution_type",
}
df_name_columns = {
    "0": "internal_name",
    "1": "common_name",
}
unpack_patterns = {
    "patch.dx11.mvgl": [
        "data/digimon_status*",
        "data/evolution*"
    ],
    "addcont_17.dx11.mvgl": [
        "data/digimon_status*",
        "data/evolution*"
    ],
    "patch_text01.dx11.mvgl": [
        "text/char_name*"
    ],
    "addcont_17_text01.dx11.mvgl": [
        "text/char_name*"
    ],
}
csv_files = {
    "digimon_status_data": [],
    "digimon_evolution_data": [],
    "digimon_name_data": []
}
csv_patterns = {
    "digimon_status_data": "digimon_status*/*_digimon_status_data.csv",
    "digimon_evolution_data": "evolution*/*_evolution_to.csv",
    "digimon_name_data": "char_name*/000_Sheet1.csv"
}

digi_ids_mode_change = []
df_digi_chart = pl.DataFrame()
df_digi_count = pl.DataFrame()
df_digi_tracker = pl.DataFrame()
df_digi_name = pl.DataFrame()
df_digi_generations = pl.DataFrame({"internal_generation": [1,2,3,4,5,6,7,8,9,10,11,12,13], "common_generation": [1,2,3,4,5,6,7,4,6,4,4,5,6]})

def print_df(df: pl.DataFrame):
    print_and_flush(''.join([f'{col:<60}' for col in df.columns]))
    for row in df.rows():
        line = ''.join([f'{"" if x is None else x:<60}' for x in row])
        print_and_flush(line)
    print_and_flush('')

def cleanup_raw_columns(df: pl.DataFrame):
    df.columns = [re.sub(r".* (\d+)", r"\1", col) for col in df.columns]
    return df

def add_to_digi_tracker(df_digi: pl.DataFrame):
    global df_digi_tracker

    df_digi_tracker = pl.concat([df_digi_tracker, df_digi])

def add_to_digi_count(df_digi: pl.DataFrame):
    global df_digi_count
    add_to_digi_tracker(df_digi)

    df_digi = df_digi.group_by("id").agg(pl.len().alias("count"))
    df_digi_count = pl.concat([df_digi_count, df_digi]).group_by("id").agg(pl.col("count").sum())

def update_digi_count(df_digi: pl.DataFrame, digi_ids_previous: list[str]=[]):
    # Update digi count
    add_to_digi_count(df_digi)

    digi_ids = df_digi["id"].unique().to_list()
    df_digi_next = df_digi.join(df_digi_chart, left_on="id", right_on="to_digimon_id")\
                          .join(df_digi_count, left_on="from_digimon_id", right_on="id")\
                          .filter(~pl.col("from_digimon_id").is_in(digi_ids_mode_change) & ~pl.col("from_digimon_id").is_in(digi_ids_previous))\
                          .with_columns(pl.col("id").is_in(FUSION_DIGIMON.values()).alias("is_fusion"))
    
    df_fusion = df_digi_next.filter(pl.col("is_fusion"))
    df_non_fusion = df_digi_next.filter(~pl.col("is_fusion")).group_by("origin_digimon_id")\
                                .agg(
                                    # When choosing a pre-digivolution for the next round, choose the one that's appeared most often already
                                    # i.e. "If you already have to farm a digimon a decent bit, then you already have a good source for them, farm a few more" typeshit typeshit
                                    pl.col("from_digimon_id").filter(pl.col("count") == pl.col("count").max()).sort().first().alias("from_digimon_id"),
                                )
    
    df_digi_next = pl.concat([df_non_fusion.select(["from_digimon_id", "origin_digimon_id"]), df_fusion.select(["from_digimon_id", "origin_digimon_id"])])\
                     .rename({"from_digimon_id":"id"})\

    if len(df_digi_next) == 0:
        return

    update_digi_count(df_digi_next, digi_ids)
    
def extract_digimon_from_save(save: str):
    regex_save_break = r"[-:+\(\)\& \w]+"
    regex_digimon_extract = r"[-:+\(\)\& \w]{3,}mon[-:+\(\) \w]*"

    with open(save["file_path"], encoding='shift_jis', errors="ignore") as file:
        content = file.read()

    matches = re.findall(regex_save_break, content)
    content_parts = [match for match in matches if match == match.encode("cp1252", errors="replace").decode("cp1252")]
    content = "\n".join(content_parts).split("\n".join([f"{n}" for n in range(10)]))[0]
    
    digis_from_save = re.findall(regex_digimon_extract, content)
    if not digis_from_save:
        return pl.DataFrame()
    
    return pl.DataFrame({"common_name": digis_from_save})\
             .group_by("common_name")\
             .agg(pl.len().alias("count"))\
             .join(df_digi_name, on="common_name", how="left")\
             .sort(["count", "common_name"], descending=[True, False])

def decrypt_save(input_file_path: str, output_file_path: str):
    key = binascii.unhexlify(ENCRYPTION_KEY)

    backend = default_backend()
    # AES-128 in ECB mode
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=backend)
    decryptor = cipher.decryptor()

    # Read input and decrypt
    with open(input_file_path, 'rb') as f_in:
        ciphertext = f_in.read()

    # Perform decryption (ECB is block-wise, so no final block necessary)
    decrypted_data = decryptor.update(ciphertext) + decryptor.finalize()

    # Write output file
    with open(output_file_path, 'wb') as f_out:
        f_out.write(decrypted_data)

def extract_data_from_game():
    for mvgl_file_name in unpack_patterns:
        # TODO: Save and uncomment when you can kick it off on game startup
        # cmd = ["./MVGLTools/MVGLToolsCLI.exe", "-g", "dsts", "-m", "unpack-mvgl", "-i", f"{GAME_DIR}/gamedata/{mvgl_file_name}", "-o", f"unpacked/mvgl_unpacked/{mvgl_file_name}"]
        # subprocess.run(cmd)
        for mbe_pattern in unpack_patterns[mvgl_file_name]:
            for file_path in glob.glob(f"unpacked/mvgl_unpacked/{mvgl_file_name}/{mbe_pattern}"):
                cmd = ["./MVGLTools/MVGLToolsCLI.exe", "-g", "dsts", "-m", "unpack-mbe", "-i", f"{file_path}", "-o", "unpacked/mbe_unpacked"]
                subprocess.run(cmd)
    for key in csv_patterns:
        csv_files[key] = glob.glob(f"unpacked/mbe_unpacked/{csv_patterns[key]}")

def check_and_extract_saves_from_game():
    new_saves = {}

    save_file_paths = [save_file for save_file in glob.glob(f"{GAME_DIR}/gamedata/savedata/*/*.bin") if re.match(r"\d{4}.bin", os.path.basename(save_file))]
    for file_path in save_file_paths:
        file_name = os.path.basename(file_path)
        if file_name not in new_saves or os.path.getmtime(file_path) != new_saves[file_name]["last_modified"]:
            new_saves[file_name] = {"file_path": f"unpacked/decrypted_saves/{file_name}", "last_modified": os.path.getmtime(file_path)}
            if not os.path.exists("unpacked/decrypted_saves"):
                os.mkdir("unpacked/decrypted_saves")
            decrypt_save(file_path, f"unpacked/decrypted_saves/{file_name}")

    return new_saves

def print_and_flush(printable):
    print(printable)
    sys.stdout.flush()

def main():
    global df_digi_chart, digi_ids_mode_change, df_digi_name
    global df_digi_count, df_digi_tracker, saves

    # Data cleanup
    # TODO: Save and uncomment when you can kick it off on game startup
    # if os.path.exists("unpacked"):
    #     shutil.rmtree("unpacked")
    # os.mkdir("unpacked")

    while True:
        new_saves = check_and_extract_saves_from_game()
        if saves != new_saves:
            saves = new_saves
            print_and_flush(saves)

            extract_data_from_game()

            # Build data frames
            df_digi_count = pl.DataFrame()
            df_digi_tracker = pl.DataFrame()
            df_digi_data = cleanup_raw_columns(pl.concat([pl.read_csv(file_path) for file_path in csv_files["digimon_status_data"]]))\
                .select(df_data_columns.keys())\
                .rename(df_data_columns)\
                .join(df_digi_generations, left_on="generation", right_on="internal_generation")\
                .select(["id","name","common_generation","is_boss"])\
                .rename({"common_generation": "generation"})
            df_digi_chart = cleanup_raw_columns(pl.concat([pl.read_csv(file_path) for file_path in csv_files["digimon_evolution_data"]]))\
                .select(df_chart_columns.keys())\
                .rename(df_chart_columns)\
                .join(df_digi_data, left_on="from_digimon_id", right_on="id", how="inner")\
                .join(df_digi_data, left_on="to_digimon_id", right_on="id", how="inner")\
                .rename({"name": "from_name", "generation": "from_generation", "name_right": "to_name", "generation_right": "to_generation"})
            df_digi_name = cleanup_raw_columns(pl.concat([pl.read_csv(file_path) for file_path in csv_files["digimon_name_data"]])).select(df_name_columns.keys()).rename(df_name_columns)
            digi_ids_mode_change = df_digi_chart.filter(pl.col("digivolution_type") == 2)["to_digimon_id"].to_list()

            generation_list = sorted(pl.concat([df_digi_chart["from_generation"], df_digi_chart["to_generation"]]).unique().to_list())

            # Calculate digimon needed for full living dex
            for gen in generation_list:
                # Handle initial case of gen 1 digimon (In-Training I)
                if gen == 1:
                    digi_gen_1 = df_digi_chart.filter(pl.col("from_generation") == 1)\
                                    .select("from_digimon_id").unique()\
                                    .with_columns(pl.col("from_digimon_id").alias("origin_digimon_id"))\
                                    .rename({"from_digimon_id":"id"})
                    add_to_digi_count(digi_gen_1)
                else:
                    digi_for_gen = df_digi_chart.filter(pl.col("to_generation") == gen)\
                                                .select("to_digimon_id").unique()\
                                                .with_columns(pl.col("to_digimon_id").alias("origin_digimon_id"))\
                                                .rename({"to_digimon_id":"id"})
                    update_digi_count(digi_for_gen)
                
            # Extract digimon from save data
            df_digi_from_save = extract_digimon_from_save(saves[CHOSEN_SAVE_FILE])
            if len(df_digi_from_save) == 0:
                continue

            df_digi_from_save = df_digi_from_save.join(df_digi_data, left_on="internal_name", right_on="name")\
                .select(df_digi_from_save.columns + ["id", "generation"])
            df_digi_count = df_digi_count.join(df_digi_data, on="id", how="left")\
                .select(["id","name", "count"])
            df_digi_tracker = df_digi_tracker.join(df_digi_data, on="id")\
                .select(["origin_digimon_id", "id", "generation"])

            # TODO: For viewing and debug, should be removed before release
            df_digi_from_save.sort("common_name").write_csv("df_digi_from_save.csv")
            df_digi_data.write_csv("df_digi_data.csv")
            df_digi_name.write_csv("df_digi_name.csv")
            df_digi_chart.write_csv("df_digi_chart.csv")
            df_digi_count.sort(["count", "name"], descending=[True, False]).write_csv("df_digi_count.csv")
            df_digi_tracker.sort(["origin_digimon_id", "generation"], descending=[False, True]).write_csv("df_digi_tracker.csv")

            # Calculate digimon needed
            # Remove all digimon from tracker that we already have
            acquired_digi_ids = df_digi_from_save["id"].to_list()
            df_digi_needed = df_digi_tracker.filter(~pl.col("origin_digimon_id").is_in(acquired_digi_ids))
            
            # Update the count from the digimon we have left
            df_digi_from_save = df_digi_from_save.with_columns(
                pl.when(pl.col("id").is_in(acquired_digi_ids))
                .then((pl.col("count")-1).alias("count"))
                .otherwise(pl.col("count"))
            ).filter(pl.col("count") > 0)
            df_digi_from_save.sort("common_name").write_csv("df_digi_from_save_2.csv")

            # Remove all digimon from tracker that we have materials for
            df_digi_needed = df_digi_needed.join(df_digi_from_save, on="id", how="left")\
                .select(["origin_digimon_id", "id", "count", "generation"])\
                .with_columns(pl.lit(False).alias("dropped"))

            for gen in sorted(generation_list, reverse=True):
                df = df_digi_from_save.filter(pl.col("generation") == gen)
                
                for sub_df in df_digi_needed.filter(pl.col("id").is_in(df["id"].to_list())).partition_by("id"):
                    # Grab the first "count"-th origin_digimon_ids
                    sub_df = sub_df.sort("origin_digimon_id").slice(0, min(len(sub_df), sub_df["count"].first()))

                    # For those origin_digimon_ids, drop all records where generation is less than the generation of the digimon id for the partition
                    df_digi_needed = df_digi_needed.with_columns(
                        pl.when(pl.col("origin_digimon_id").is_in(sub_df["origin_digimon_id"].to_list()) & pl.col("generation").le(sub_df["generation"].first()))
                        .then(True)
                        .otherwise(pl.col("dropped"))
                        .alias("dropped")
                    )

            df_digi_needed = df_digi_needed.filter(~pl.col("dropped"))\
                                           .group_by("id")\
                                           .agg(pl.len().alias("count"))\
                                           .join(df_digi_data, on="id")\
                                           .join(df_digi_name, left_on="name", right_on="internal_name")\
                                           .select(["id", "common_name", "count"])\
                                           .rename({"common_name": "name"})
            
            df_digi_needed = df_digi_needed.join(df_digi_data, on="id")\
                                           .select(["id", "name", "generation", "count"])

            df_digi_needed.sort(["generation", "count", "name"], descending=[True, True, False]).write_csv("df_digi_needed.csv")

            digi_from_save_unpaired = df_digi_from_save.filter(pl.col("internal_name").is_null()).sort("common_name")
            print_and_flush(f"Unmatched Digimon in Save: {len(digi_from_save_unpaired)}")
            if len(digi_from_save_unpaired) > 0:
                print_df(digi_from_save_unpaired)

        time.sleep(1)
        
if __name__ == "__main__":
    main()
