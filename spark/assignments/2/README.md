## **Assignment #2**
### **Spark WordCount Application**
____


#### **Instructions**
____
For your second assignment I would like you to write code that does a word and character count on some input:

+ URL
+ Input File

The word count should not only count all occurrences of an individual word, the total number of representations of that word, and a frequency analysis of that word versus the total document.

Occurence are considered for spelling, without care of capitalization

for example -- **"word Word word WoRd apple":**

+ word 80% 4 total occurrences, 3 representations {word, Word, WoRd,}

+ apple 20% 1 total occurence, 1 representation {apple}

The character count similar analyzes the occurrences, representations and frequency analysis of character in the document.

#### **Implementation**
____

+ **Program :** [wordcount.py](wordcount.py)

+ **How to use it :**

```
python wordcount.py --help
usage: wordcount.py [-h] [--print_to_screen] [--from_url] [--from_file]  [--to_csv_file]

Spark WordCount Application

optional arguments:
  -h, --help          show this help message and exit
  --print_to_screen   print analysis result to screen
  --from_url          URL of file
  --from_file         Path of file
  --to_csv_file       Path to save the result in a CSV file

```

#### **Examples**
____

+ **Using a sample dataset and printing the result to the screen**
    + Input
        ```
        python wordcount.py --print_to_screen true
        ```

    + Output
        ```
        Spark WordCount Application
        Big Data Programming - Spring 2020
        Georgia State University

        Parameters:
            Print analysis result to screen...: True
            Dataset URL.......................: None
            Dataset file location.............: None
            Using random dataset..............: True
            Export to CSV file................: None

            20/02/11 16:22:05 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
            Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
            Setting default log level to "WARN".
            To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
        {                                                                               
            "word": {
                "occurrences": 4,
                "representations": [
                    "word",
                    "Word",
                    "WoRd"
                ],
                "percentage": 0.8
            },
            "apple": {
                "occurrences": 1,
                "representations": [
                    "apple"
                ],
                "percentage": 0.2
            }
        }
        Total of words : 5
        Process completed in 8.645995 second(s)
        ```

+ **Using a URL and printing the result to the screen**
    ```
    python wordcount.py --print_to_screen true --from_url="http://www.thegrammarlab.com/?wpdmpro=fiction-sample&wpdmdl=630"
    ```

+ **Using a local file and saving the results to a local CSV file**
    ```
    python wordcount.py --from_file=resources/sample1.txt --to_csv_file=/tmp/result.txt --print_to_screen=false
    ```

#### **Additional information**
____
+ List top 10 words by occurrences in decendent order, use:
    
    ```bash
    cat /tmp/result.txt | sed '1d'| sort -t, -nrk 2| head -n 10

    ```
