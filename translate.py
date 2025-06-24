import pandas as pd
from googletrans import Translator
from tqdm import tqdm
import time
from httpx import Timeout
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from langdetect import detect, DetectorFactory
import re
import random
# Creates Translator object
def create_translator():
    translator = Translator(timeout=Timeout(10.0))
    translator.raise_Exception = True
    return translator


def translate(translator, transcription, max_retries=5, delay=1):
    for retry in range(max_retries):
        try:
            translated_text = translator.translate(transcription, dest='en').text
            if translated_text:
                time.sleep(delay)  # Introducing the delay
                return translated_text
        except Exception as e:
            if "429" in str(e):  # If we encounter a 429 error
                sleep_time = (2 ** retry) + random.random()  # exponential backoff with jitter
                print(f"Rate limit exceeded. Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
                continue
            elif "JSON object must be str, bytes or bytearray" in str(e):
                print("Received unexpected response from the translation service. Skipping this translation...")
                return "Translation Error"
            elif "list index out of range" in str(e):  # This is for the list index error
                print("List index out of range error. Skipping this translation...")
                return "Translation Error"
            elif retry < max_retries - 1:  # i.e. not the last retry
                print(f"Error: {e}. Retrying...")
                time.sleep(2 ** retry)  # Exponential backoff
                continue
            else:
                print(f"Failed to translate after {max_retries} attempts. Error: {e}")
                return "Translation Error"




def validate_text(text):

    # Trim the text
    trimmed_text = text.strip()

    # Check if it's empty or too short
    if not trimmed_text or len(trimmed_text) < 2:  # Here, I'm assuming a minimum length of 2 characters
        return None

    return trimmed_text


# Calculates English percentage in the text
def calculate_english_percentage(text, group_size=20):
    words = re.findall(r'\w+', text)
    groups = [" ".join(words[i:i + group_size]) for i in range(0, len(words), group_size)]

    english_count = 0
    total_groups = len(groups)

    for group in groups:
        try:
            if detect(group) == 'en':
                english_count += 1
        except:
            pass

    if total_groups > 0:
        return english_count / total_groups
    else:
        return 0

# Determines if the text is predominantly English
def is_english_dominant(text, threshold=0.8):
    english_percentage = calculate_english_percentage(text)
    return english_percentage >= threshold


# Find the translation for the text
def find_translation(trans):
    if not str:
        return ""
    trans = str(trans)
    trans = validate_text(trans)

    if not trans:
        return "Invalid Text"

    if is_english_dominant(trans):
        return trans

    limit = 4800
    exception_count_trans = 0

    with ThreadPoolExecutor(max_workers=8) as executor:
        while True:
            if exception_count_trans == 10:
                break

            try:
                length = len(trans)
                if length <= limit:
                    return translate(translator, trans)
                else:
                    return traverse(length, limit, trans, translate, translator)
            except Exception as exc:
                exception_count_trans += 1
                print("Exception happened while translating the language: ", exc)
                limit = int(limit / 2)

# Traverses the data (like batches) since there is a characterization limit.
def traverse(length, limit, trans, function, translator):
    complete_text = ""
    loop_count = int(length/limit)

    for i in range(loop_count):
        text = ""
        if i == 0:
            text = function(translator, trans[:limit*(i+1)])
        elif i == loop_count-1:
            text = function(translator, trans[limit*(i+1):])
        else:
            text = function(translator, trans[limit*i:limit*(i+1)])

        complete_text += text

    return complete_text

def multiprocess_video(attribute, func):
    with Pool(8) as p:
        return list(tqdm(p.imap(func, attribute), total=len(attribute), desc='Translating the language'))


def translate_en(filename, doc, attribute = 'transcription'):
    df = pd.read_csv(filename+".csv")

    if doc == 'whisper':
        df = df[df[attribute] != 'Not Video']
        df = df[df[attribute].notnull()]
        df.reset_index(drop=True, inplace=True)

    translations = multiprocess_video(df[attribute], find_translation)
    df[attribute] = translations

    #trans_filtered_df = df[df[attribute] != "Not English"]

    valid_df = df[df[attribute] != "Invalid Text"]
    print("Invalid Texts are Dropped:", len(df) - len(valid_df))

    no_error_valid_df = valid_df[valid_df[attribute] != "Translation Error"]
    print("Translation Errors are Dropped:", len(valid_df) - len(no_error_valid_df))

    no_error_valid_df.to_csv(filename + "_translated.csv", index=False)

    return no_error_valid_df
translator = create_translator()
# Ensure consistent results
DetectorFactory.seed = 0

if __name__ == "__main__":
    translate_en('asonam_scs_engagements_subset_comment_combined', doc="comment", attribute='comment_combined')
