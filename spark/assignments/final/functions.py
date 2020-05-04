import datetime

from pyspark.sql import Row, SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructField,StructType,StringType, IntegerType, DoubleType

ML_DIR='models/lr'
EXPORT_PRED='resources/'

def process(time, rdd):    
    spark = SparkSession.builder.appName('processLR').getOrCreate()
    
    rows = []
    data_schema = [StructField('Avg Session Length', DoubleType(), False),
                   StructField('Time on App', DoubleType(), False),
                   StructField('Time on Website', DoubleType(), False),
                   StructField('Length of Membership', DoubleType(), False)
                  ]

    data_struct = StructType(fields=data_schema) 

    header = ['Avg Session Length', 'Time on App', 'Time on Website', 'Length of Membership']
    
    for row in rdd.map(lambda line: line.split(",")).collect():
        rows.append((*[float(i) for i in row],))
    
    if (len(rows)) > 0:
        dataFrame = spark.createDataFrame(rows, data_struct)
        assembler = VectorAssembler(inputCols=header, outputCol='features')
        output = assembler.transform(dataFrame)

        master = output.select(['features'])

        model = LinearRegressionModel.load(ML_DIR)
        predictions = model.transform(master)

        predictions.show()

        ts = datetime.datetime.now()
        predictions.select('predictions').write.format('csv').save(f'{EXPORT_PRED}predictions-{ts.year}-{ts.month}-{ts.day}-{ts.hour}-{ts.minute}-{ts.second}')






