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
    global print_to_screen
    global from_url
    global from_file
    global from_random
    global to_csv_file
    global start_exec    

    start_exec = datetime.datetime.now()

    parser = argparse.ArgumentParser(description='Spark WordCount Application')
    parser.add_argument('--print_to_screen',type=lambda x: (str(x).lower() in ['true','1', 'yes']), metavar='', required=False, help='print analysis result to screen', default=True)
    parser.add_argument('--from_url',type=str, metavar='', required=False, help='URL of file', default=None)
    parser.add_argument('--from_file',type=str, metavar='', required=False, help='Path of file', default=None)
    parser.add_argument('--to_csv_file',type=str, metavar='', required=False, help='Path to save the result in a CSV file', default=None)

    args = vars(parser.parse_args())

    print_to_screen = args['print_to_screen']
    from_url = args['from_url']
    from_file = args['from_file']
    to_csv_file = args['to_csv_file']

    if (from_url is None and from_file is None):
        from_random = True

    else:
        from_random = False

    splash_screen()


def splash_screen():
    clear()
    
    print (f'''
Spark WordCount Application
    Big Data Programming - Spring 2020
    Georgia State University

Parameters:
    Print analysis result to screen...: {print_to_screen}
    Dataset URL.......................: {from_url}
    Dataset file location.............: {from_file}
    Using random dataset..............: {from_random}
    Export to CSV file................: {to_csv_file}
''')    

def get_rdd():
    sample_rdd = ['word', 'Word', 'word', 'WoRd', 'apple']

    if (from_file):
        return from_file

    elif (from_url):
        r = requests.get(from_url)

        if r.status_code == requests.codes.ok:
            r.encoding='utf-8'

            with tempfile.NamedTemporaryFile(delete=False, mode="w+") as f:
                f.writelines(r.text)
                f.flush()

            return f.name

    else:
        return sample_rdd

def get_sc():
    try:
        conf = SparkConf()\
            .setMaster('local')\
            .setAppName('Assignment 2')

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
    field_names = ['word', 'percentage', 'occurrences', 'representations']

    with open(csv_file_name, "w") as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        
        writer.writeheader()

        for k,v in stats.items():
            row = {
                'word'            : k,
                'percentage'      : v['percentage'],
                'occurrences'     : v['occurrences'],
                'representations' : ",".join(v['representations'])
            }

            writer.writerow(row)

def process():
    stats_words = {}
    stats_char = {}
    total_words = 0
    total_char = 0

    sc = get_sc()

    if (sc):
        data_source = get_rdd()
        
        if (from_random):        
            rdd = sc.parallelize(data_source)

        else:
            rdd = sc.textFile(data_source)

        # generate main rdd
        words = rdd.flatMap(split_word)
        characters = words.flatMap(lambda word: word)

        total_words = words.count()
        total_char = characters.count()

        # process words list
        for record in words.map(lambda item: (item.strip().lower(), item)).reduceByKey(lambda k,v: k+","+v).collect():
            key   = record[0]
            value = record[1].split(',')

            stats_words[key] = {
                'occurrences'     : len(value),
                'representations' : list(set(value)),
                'percentage'      : len(value)/total_words
            }

        # process character list
        for record in characters.map(lambda char: (char.lower(), char)).reduceByKey(lambda k,v: k+","+v).collect():
            key   = record[0]
            value = record[1].split(',')

            stats_char[key] = {
                'occurrences'     : len(value),
                'representations' : list(set(value)),
                'percentage'      : len(value)/total_char
            }


        if (to_csv_file):
            save_to_csv(stats_words, to_csv_file.split(".")[0] + "_word_stats." + to_csv_file.split(".")[1])
            save_to_csv(stats_char, to_csv_file.split(".")[0] + "_word_char." + to_csv_file.split(".")[1])

        if (print_to_screen):
            print ("Words statistics")
            print(json.dumps(stats_words,indent=4))  

            print ("Characters statistics")
            print(json.dumps(stats_char,indent=4))  

            print(f"Total of words........: {total_words}")
            print(f"Total of characters...: {total_char}")

        sc.stop()

def end():
    seconds_elapsed = (datetime.datetime.now() - start_exec).total_seconds()
    print (f"Process completed in {seconds_elapsed} second(s)")