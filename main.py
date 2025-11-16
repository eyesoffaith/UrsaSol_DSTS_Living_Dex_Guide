# TODO: Consider re-writting without using DataFrames. Not worth the performance gains likely
# TODO: Digimon with mode changes between the same generation are not properly recorded (i.e. Ceresmon/Cersmon Medium, Bacchusmon/Bacchusmon DM)
# TODO: Decrypt live save, check for progress of live dex and adjust needed digimon.
#   - Force reading as UTF-8 and swallowing the UnicodeEncodeError gets a decent result if we're just tracking the Digimon the player has obtained

import polars as pl
import re
import os
import shutil
import subprocess
import glob
import binascii

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding

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

GAME_DIR = "P:\Program Files (x86)\Steam\steamapps\common\Digimon Story Time Stranger"
CHOSEN_SAVE_FILE = "0006.bin"

# TODO: Fix file extraction
mvgl_file_names = ["patch.dx11", "addcont_17.dx11"]
mbe_patterns = ["digimon_status*", "evolution*"]

fusion_digimon = {"char_DINOBEEMON":23, "char_OMEGAMON":88, "char_SUSANOOMON":104, "char_CHAOSMONVALDURARM":118, "char_EXAMON":215, "char_MILLENNIUMON":230, "char_PAILDRAMON":408, "char_GRACENOVAMON":604, "char_SILPHYMON":720, "char_SHAKKOUMON":723, "char_MASTEMON":748, "char_ALPHAMON_OURYUKEN":766, "char_CHAOSMON":772, "char_SKULLBALUCHIMON_TITAMON":915, "char_ENBARRMON_CRANIAMON":494, "char_OMEGAMON_ZWART":757}

digi_ids_mode_change = []
df_digi_chart = pl.DataFrame()
df_digi_count = pl.DataFrame()
df_digi_tracker = pl.DataFrame()

def print_df(df: pl.DataFrame):
    print(''.join([f'{col:<60}' for col in df.columns]))
    for row in df.rows():
        line = ''.join([f'{x or "":<60}' for x in row])
        print(line)
    print('')

def cleanup_raw_columns(df: pl.DataFrame):
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
    # print_df(df_digi)

    df_digi_count = pl.concat([df_digi_count, df_digi]).group_by("id").agg(pl.col("count").sum())
    # print("Result")
    # print_df(df_digi_count)

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
    
def extract_digimon_from_save(file_path: str):
    regex_save_break = r"[-:+\(\)\& \w]+"
    regex_digimon_extract = r"[-:+\(\)\& \w]{3,}mon[-:+\(\) \w]*"

    with open(file_path, encoding='shift_jis', errors="ignore") as file:
        content = file.read()

        matches = re.findall(regex_save_break, content)
        content_parts = [match for match in matches if match == match.encode("cp1252", errors="replace").decode("cp1252")]
        content = "\n".join(content_parts).split("\n".join([f"{n}" for n in range(10)]))[0]
        
        return re.findall(regex_digimon_extract, content)

def decrypt_save(input_file_path: str, output_file_path: str):
    ENCRYPTION_KEY = "33393632373736373534353535383833"
    key = binascii.unhexlify(ENCRYPTION_KEY)

    backend = default_backend()
    # AES-128 in ECB mode
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=backend)
    decryptor = cipher.decryptor()

    # 3. Read input and decrypt
    with open(input_file_path, 'rb') as f_in:
        ciphertext = f_in.read()

    # 4. Perform decryption (ECB is block-wise, so no final block necessary)
    decrypted_data = decryptor.update(ciphertext) + decryptor.finalize()

    # 5. Write output file
    with open(output_file_path, 'wb') as f_out:
        f_out.write(decrypted_data)

def main():
    global df_digi_chart, df_digi_count, digi_ids_mode_change

    # TODO: Save and uncomment when you can kick it off on game startup
    # if os.path.exists("./unpacked"):
    #     shutil.rmtree("./unpacked")
    for mvgl_file_name in mvgl_file_names:
        # TODO: Save and uncomment when you can kick it off on game startup
        # cmd = ["./MVGLTools/MVGLToolsCLI.exe", "-g", "dsts", "-m", "unpack-mvgl", "-i", f"{GAME_DIR}/gamedata/{mvgl_file_name}.mvgl", "-o", f"unpacked/{mvgl_file_name}.mvgl"]
        # subprocess.run(cmd)
        for mbe_pattern in mbe_patterns:
            for file_path in glob.glob(f"unpacked/{mvgl_file_name}.mvgl/data/{mbe_pattern}.mbe"):
                cmd = ["./MVGLTools/MVGLToolsCLI.exe", "-g", "dsts", "-m", "unpack-mbe", "-i", f"{file_path}", "-o", "unpacked"]
                subprocess.run(cmd)
    digimon_status_files = glob.glob(f"unpacked/*/*_digimon_status_data.csv")
    digimon_evolution_to_files = glob.glob(f"unpacked/*/*_evolution_to.csv")

    # Extract digimon from save data
    df_name_translate = pl.read_csv("data/digi_name_translate.csv")

    save_files = [save_file for save_file in glob.glob(f"{GAME_DIR}/gamedata/savedata/*/*.bin") if re.match(r"\d{4}.bin", os.path.basename(save_file))]
    for file_path in save_files:
        file_name = os.path.basename(file_path)
        if not os.path.exists("unpacked/decrypted_saves"):
            os.mkdir("unpacked/decrypted_saves")
        decrypt_save(file_path, f"unpacked/decrypted_saves/{file_name}")

    digis_from_save = extract_digimon_from_save(f"unpacked/decrypted_saves/{CHOSEN_SAVE_FILE}")
    if len(digis_from_save) == 0:
        return

    df_digi_from_save = pl.DataFrame({"common_name": digis_from_save}).group_by("common_name")\
                                                                      .agg(pl.len().alias("count"))\
                                                                      .join(df_name_translate, on="common_name", how="left")\
                                                                      .sort("count", descending=[True])
    print_df(df_digi_from_save)

    # Build evolution data frames
    df_digi_data = cleanup_raw_columns(pl.concat([pl.read_csv(file_path) for file_path in digimon_status_files])).select(df_data_columns.keys()).rename(df_data_columns)

    df_digi_chart = cleanup_raw_columns(pl.concat([pl.read_csv(file_path) for file_path in digimon_evolution_to_files])).select(df_chart_columns.keys()).rename(df_chart_columns)
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="from_digimon_id", right_on="id", how="inner").rename({"name": "from_name", "generation": "from_generation"})
    df_digi_chart = df_digi_chart.join(df_digi_data, left_on="to_digimon_id", right_on="id", how="inner").rename({"name": "to_name", "generation": "to_generation"})

    # TODO: For viewing and debug, should be removed for release
    df_digi_data.write_csv("df_digi_data.csv")
    df_digi_chart.write_csv("df_digi_chart.csv")

    digi_ids_mode_change = df_digi_chart.filter(pl.col("digivolution_type") == 2)["to_digimon_id"].to_list()

    # TODO: For viewing and debug, should be removed for release
    digi_from_save_unpaired = df_digi_from_save.filter(pl.col("internal_name").is_null()).sort("common_name")
    print(f"Unmatched Digimon in Save: {len(digi_from_save_unpaired)}")
    if len(digi_from_save_unpaired) > 0:
        print_df(digi_from_save_unpaired)

    generation_list = pl.concat([df_digi_chart["from_generation"], df_digi_chart["to_generation"]]).unique().to_list()
    generation_list.sort()
    generation_list.remove(1) # remove gen 1, handled as a special case

    # Handle initial case of gen 1 digimon (In-Training I)
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

if __name__ == "__main__":
    main()
