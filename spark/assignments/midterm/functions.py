import re
import json
import argparse
import datetime
import os
import tempfile
import requests
import io
import csv

from pyspark import SparkConf, SparkContext

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def init():
    global encode_file
    global decode_file
    global without_report
    global without_decrypt
    global decrypt_key

    global start_exec    

    start_exec = datetime.datetime.now()

    parser = argparse.ArgumentParser(description='Spark Caesar Cypher Application')

    req = parser.add_argument_group('Required arguments')
    req.add_argument('--encode-file', type=str, metavar='<file>', required=True, help='File location of the encoded document', default='')
    req.add_argument('--decode-file', type=str, metavar='<path>', required=True, help='Path to save the decoded document', default='resources/<file_name>_decoded.csv')    

    opt = parser.add_argument_group('Optional arguments')
    opt.add_argument('--without-report', type=lambda x: (str(x).lower() in ['true','1', 'yes']), metavar='<boolean>', required=False, help='Dont generate letter frequency report', default=False)
    opt.add_argument('--without-decrypt', type=lambda x: (str(x).lower() in ['true','1', 'yes']), metavar='<boolean>', required=False, help='Dont generate decode document', default=False)
    opt.add_argument('--decrypt-key', type=int, metavar='<integer>', required=False, help='Decode the document based on <decrypt-key> number', default=None)

    args = vars(parser.parse_args())

    encode_file = args['encode_file']
    decode_file = args['decode_file']

    without_report = args['without_report'] if (not args['without_report'] is None) else False
    without_decrypt = args['without_decrypt'] if (not args['without_decrypt'] is None) else False
    decrypt_key = args['decrypt_key'] if (not args['decrypt_key'] is None) else 'auto'

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
    Without letter frequency report generation...: {without_report}
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


def process():
    stats_words = {}
    stats_char = {}
    total_words = 0
    total_char = 0

    sc = get_sc()

    if (sc):
        sc.stop()



def end():
    seconds_elapsed = (datetime.datetime.now() - start_exec).total_seconds()
    print (f"Process completed in {seconds_elapsed} second(s)")