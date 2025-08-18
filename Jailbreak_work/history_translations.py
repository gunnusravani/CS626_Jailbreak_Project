import os
import pandas as pd
from tqdm import tqdm
import time
from googletrans import Translator
from nltk.tokenize import sent_tokenize

def extract_language_from_path(path):
    parts = path.split(os.sep)
    return parts[-2] if len(parts) >= 2 else None

def map_language_name_to_code(lang_name):
    mapping = {
        "hindi": "hi",
        "bengali": "bn",
        "telugu": "te",
        "marathi": "mr",
        "english": "en"
    }
    return mapping.get(lang_name.lower(), None)



def translate_sentence_safe(sentence, src_lang, translator, max_retries=2):
    sentence = str(sentence)

    def try_translate(s, src):
        for attempt in range(max_retries):
            try:
                translated = translator.translate(s, src=src, dest="en")
                return translated.text if translated else None
            except Exception as e:
                print(f"Error translating with src={src} (attempt {attempt+1}): {e}")
                time.sleep(1.5 * (attempt + 1))
        return None

    # First try full sentence
    result = try_translate(sentence, src_lang)

    # Fallback: try with src='en'
    if result is None and src_lang != "en":
        result = try_translate(sentence, "en")

    # Still failing? Try splitting into two sentences
    if result is None:
        print("🔁 Attempting sentence split and translation...")
        try:
            sentences = sent_tokenize(sentence)
            if len(sentences) >= 2:
                first_half = " ".join(sentences[:len(sentences)//2])
                second_half = " ".join(sentences[len(sentences)//2:])
                
                first_trans = try_translate(first_half, src_lang) or try_translate(first_half, "en")
                second_trans = try_translate(second_half, src_lang) or try_translate(second_half, "en")

                if first_trans and second_trans:
                    result = first_trans + " " + second_trans
                elif first_trans:
                    result = first_trans + " " + second_half
                elif second_trans:
                    result = first_half + " " + second_trans


                else:
                    print(f"❌ Split translation failed:\n{sentence[:150]}...")
                    result = None
                print("result sent successfully")
#                 print(result)
            else:
                print("⚠️ Could not split into two sentences. Skipping.")
        except Exception as e:
            print(f"❌ Sentence splitting error: {e}")
            result = None

    return result

def translate_columns(file_path, translator, column_pairs):
    try:
        df = pd.read_csv(file_path)
        col = df.columns

        # Rename fallback: if some expected cols aren't found but alternatives exist
        if "trans_response" in col and "response" not in col:
            df = df.rename(columns={"trans_response": "response"})
            print("Column renamed: trans_response → response")

        lang_folder = extract_language_from_path(file_path)
        src_lang = map_language_name_to_code(lang_folder)

        if not src_lang:
            print(f"⚠️ Skipping {file_path}: Unknown language '{lang_folder}'")
            return

        print(f"🔵 Processing file: {file_path} | Source language: {src_lang}")

        for src_col, tgt_col in column_pairs:
            if src_col not in df.columns:
                print(f"⚠️ Skipping column {src_col} in {file_path}: Column not found.")
                continue

            if tgt_col not in df.columns:
                df[tgt_col] = ""

            needs_translation = (
                df[tgt_col].isnull() | (df[tgt_col].astype(str).str.strip() == "")
            ) & (df[src_col].astype(str).str.strip() != "")

            rows_to_translate = df[needs_translation].copy()

            if not rows_to_translate.empty:
                print(f"🔄 Translating {len(rows_to_translate)} rows for column '{src_col}'...")
                translated_texts = []

                for response in tqdm(rows_to_translate[src_col], desc=f"Translating {src_col}", ncols=100):
                    translated_text = translate_sentence_safe(response, src_lang, translator)
                    translated_texts.append(translated_text if translated_text is not None else response)

                df.loc[needs_translation, tgt_col] = translated_texts
            else:
                print(f"✅ No rows need translation in column '{src_col}'.")

        df.to_csv(file_path, index=False)
        print(f"✅ File saved: {file_path}")

    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")

def process_directory(root_dir):
    translator = Translator()
    column_pairs = [
        ("initial_response", "gtrans_initial_response"),
        ("final_response", "gtrans_final_response")
    ]

    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".csv") and "checkpoint" not in filename.lower():
                file_path = os.path.join(dirpath, filename)
                translate_columns(file_path, translator, column_pairs)
            else:
                if "checkpoint" in filename.lower():
                    print(f"⏩ Skipping checkpoint file: {filename}")

if __name__ == "__main__":
    root_directory = "../jailbreak_responses_final/history/gemma3_12b"
    process_directory(root_directory)
