import re
import json
import argparse
import datetime
import os
import io
import csv
import pandas as pd
import nltk
import shutil


from pyspark import SparkConf, SparkContext
from nltk.corpus import words

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def init():
    global encode_file
    global decode_file
    global decode_rdd
    global without_decrypt
    global decrypt_key
    global freq_en_letters
    global freq_sample_size
    global total_words_attempts

    global start_exec    

    start_exec = datetime.datetime.now()

    parser = argparse.ArgumentParser(description='Spark Caesar Cypher Application')

    req = parser.add_argument_group('Required arguments')
    req.add_argument('--encode-file', type=str, metavar='<file>', required=True, help='File location of the encoded document', default='')    

    opt = parser.add_argument_group('Optional arguments')    
    opt.add_argument('--without-decrypt', type=lambda x: (str(x).lower() in ['true','1', 'yes']), metavar='<boolean>', required=False, help='Dont decrypt encoded document', default=False)
    opt.add_argument('--decrypt-key', type=int, metavar='<integer>', required=False, help='Decode the document based on <decrypt-key> number', default=None)
    opt.add_argument('--decode-file', type=str, metavar='<path>', required=False, help='Path to save the decoded text document', default=None)    
    opt.add_argument('--decode-rdd', type=str, metavar='<path>', required=False, help='Directory to save the decoded RDD partition', default=None)    
    opt.add_argument('--freq-en-letters', type=str, metavar='<path>', required=False, help='Path of frequency English letters file', default=None)
    opt.add_argument('--freq-sample-size', type=float, metavar='<float>', required=False, help='Sample size used by auto decrypt', default=None)
    opt.add_argument('--total-attempts', type=float, metavar='<float>', required=False, help='Minimum numbers of attempts used by auto decrypt to define is a word is a English word', default=None)

    args = vars(parser.parse_args())

    encode_file = args['encode_file']    

    decode_file = args['decode_file'] if (not args['decode_file'] is None) else f'resources/{os.path.basename(os.path.splitext(encode_file)[0])+"_decoded.txt"}'
    decode_rdd = args['decode_rdd'] if (not args['decode_rdd'] is None) else f'output/rdd/{os.path.basename(os.path.splitext(encode_file)[0])+"/"}'
    without_decrypt = args['without_decrypt'] if (not args['without_decrypt'] is None) else False
    decrypt_key = args['decrypt_key'] if (not args['decrypt_key'] is None) else 'auto'
    freq_en_letters = args['freq_en_letters'] if (not args['freq_en_letters'] is None) else 'resources/letter_frequency_en_US.csv'
    freq_sample_size = args['freq_sample_size'] if (not args['freq_sample_size'] is None) else 0.05
    total_words_attempts = args['total_attempts'] if (not args['total_attempts'] is None) else 5

    nltk.download('words')

    splash_screen()

def splash_screen():
    clear()
    
    print (f'''
Spark Caesar Cypher Application
    Big Data Programming - Spring 2020
    Georgia State University

Parameters:
    File location of the encoded document........: {encode_file}
    Path to save the decrypted document..........: {decode_file}
    Path location of RDD decoded partition.......: {decode_rdd}
    Without decode document generation...........: {without_decrypt}
    Decryption key...............................: {decrypt_key}    
''')    

def get_sc():
    try:
        conf = SparkConf()\
            .setMaster('local')\
            .setAppName('Midterm')

        return SparkContext(conf=conf).getOrCreate()

    except Exception as e:
        print(f"Error creating Spark context - {e}")

    return None


def split_word(content):
    REG_EXPR = "'?([_-a-zA-z0-9']+)'?"

    pattern = re.compile(r'{}'.format(REG_EXPR))
    matches = pattern.finditer(content)
    words   = []

    for match in matches:
        words.append(match.group(0))

    return words


def save_to_csv(stats, csv_file_name):
    field_names = ['letter', 'percentage', 'occurrences', 'representations']

    with open(csv_file_name, "w") as f:
        writer = csv.DictWriter(f, fieldnames=field_names)

        writer.writeheader()

        for k,v in stats.items():
            row = {
                'letter'          : k,
                'percentage'      : v['percentage'],
                'occurrences'     : v['occurrences'],
                'representations' : ",".join(v['representations'])
            }

            writer.writerow(row)


def get_keys_decode_order(freq_report):
    prefer_keys_order = []

    letters = pd.read_csv(freq_report)
    cond = letters['letter'].str.contains('[A-Z]')
    ceaser_letters = letters[cond].copy(deep=True)

    ceaser_letters.sort_values(by=['occurrences'],ascending=False, inplace=True)

    en_letters = pd.read_csv(freq_en_letters)
    en_letters.sort_values(by=['count'],ascending=False, inplace=True)

    for i in range(en_letters.shape[0]):
        en_key   = en_letters['letter'].values[i]
        dec_key  = ceaser_letters['letter'].values[i]    
        key_size = abs(ord(dec_key) - ord(en_key))

        prefer_keys_order.append(key_size)
    
    decode_order  =  list(dict.fromkeys(prefer_keys_order))
    missing_items = [x for x in range(26) if x not in decode_order]

    return (decode_order + missing_items)


def get_shifted_letter(new_key, letter):
    
    cypher_alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    return (cypher_alpha[new_key] if letter in cypher_alpha else letter)    

def encrypt(key, message):
    cypher_alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    message = message.upper()
    result = ""

    for letter in message:
        new_key = (cypher_alpha.find(letter) + key) % len(cypher_alpha)
        result = result + get_shifted_letter(new_key, letter)

    return result

def decrypt(key, message):
    cypher_alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    message = message.upper()
    result = ""

    for letter in message:
        new_key = (cypher_alpha.find(letter) - key) % len(cypher_alpha)
        result = result + get_shifted_letter(new_key, letter)

    return result


def auto_decode_word(sample_words, prefer_keys_order):

    total_found = 0
    decode_key = -1

    for key in prefer_keys_order:
        total_found = 0
        
        for word in sample_words:         

            decrypt_word = decrypt(key, word)

            if decrypt_word.lower() in words.words():
                total_found = total_found + 1
            
            if total_found >= total_words_attempts:
                return key
    
    return decode_key

def clear_rdd_output(dir_path):

    try:
        if os.path.isdir(dir_path):
            shutil.rmtree(dir_path)
    except :
        pass       


def process():
    stats_words = {}
    stats_char = {}
    total_words = 0
    total_char = 0

    sc = get_sc()

    if (sc):
        rdd = sc.textFile(encode_file)

        # generate main rdd
        words = rdd.flatMap(split_word)
        characters = words.flatMap(lambda word: word)

        total_words = words.count()
        total_char = characters.count()

        # process words list
        filter_words = words.map(lambda item: (item.strip().upper(), item)).reduceByKey(lambda k,v: k+","+v)

        for record in filter_words.collect():
            key   = record[0]
            value = record[1].split(',')

            stats_words[key] = {
                'occurrences'     : len(value),
                'representations' : list(set(value)),
                'percentage'      : len(value)/total_words
            }

        # process character list
        for record in characters.map(lambda char: (char.upper(), char)).reduceByKey(lambda k,v: k+","+v).collect():
            key   = record[0]
            value = record[1].split(',')

            stats_char[key] = {
                'occurrences'     : len(value),
                'representations' : list(set(value)),
                'percentage'      : len(value)/total_char
            }

        # generate frequency report
        freq_report = f'resources/{ os.path.basename(os.path.splitext(encode_file)[0])}_frequency_letters.csv'
        save_to_csv(stats_char, freq_report)

        # construct key list based on frequency letters
        prefer_keys_order = []

        if (decrypt_key == 'auto'):
            prefer_keys_order = get_keys_decode_order(freq_report)

        else:
            prefer_keys_order.append(decrypt_key)

        # decrypt document  
        if (without_decrypt == False):
            sample_words = []
            sample_words = [x[0] for x in filter_words.sample(False, freq_sample_size).collect()]

            key_size = auto_decode_word(sample_words, prefer_keys_order)
            
            if ( key_size >= 0):
                output_rdd = rdd.map(lambda line: decrypt(key_size, line))

                with open(decode_file, 'w') as writer:                    
                    rdd_lines = output_rdd.collect()

                    for line in rdd_lines:                    
                        writer.write (line + '\n')

                clear_rdd_output(decode_rdd)
                output_rdd.saveAsTextFile(decode_rdd)

            else:
                print("Unable to auto decrypt the file. Please verify if the original document was written & encoded in English.")

        sc.stop()



def end():
    seconds_elapsed = (datetime.datetime.now() - start_exec).total_seconds()
    print (f"Process completed in {seconds_elapsed} second(s)")