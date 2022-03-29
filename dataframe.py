from py4j.protocol import Py4JJavaError
from instance import *
from support import *
import numpy as np
from pyspark.sql import SparkSession, Window
from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame as pysparkDf
import pyspark.sql.functions as F

class DataFrame:
    
    spark = None
    
    def __init__(self, obj=None, columns=None, spark=None):
        
        if spark is None:
            spark = SparkSession.builder.getOrCreate()

        if isinstance(df_arr, pysparkDf):
            self.obj = obj
        elif isinstance(obj, DataFrame):
            self.obj = obj.obj

        elif is_list_of_str_num(obj):
            self.obj = spark.createDataFrame(np.expand_dims(obj, axis=1).tolist(), columns=(columns if not columns else '_c1'))
        
        elif isinstance(obj, list) and all([is_list_of_str_num(sub_obj) for sub_obj in obj]):
            self.obj = spark.createDataFrame(obj, columns=(columns if len(columns)==len(obj) else ['_c'+str(i) for i in range(len(obj))]))

        raise Exception("Only DataFrame type is supported for now")
    
    def read_csv(self, s3_filepath, header=True):
        
        self.obj = spark.read.option("header",str(header).lower()).csv(s3_filepath)
        
    def read_parquet(self, s3_filepath):
        
        self.obj = spark.read.parquet(s3_filepath)
    
    def __getitem__(self, key):

        if is_str_num(key):
            if key in self.obj.columns:
                return spark.select(key)
            else:
                raise Exception("No key {} in DataFrame".format(key))
        
        elif is_list_of_str_num(key):
            if len(set(key).intersection(set(self.obj.columns))) == len(key):
                return spark.select(key)
            else:
                raise Exception("No key {} in DataFrame".format(key))

        raise Exception("others not implemented yet")

    def __setitem__(self, key, value):

        if is_str_num(key):
            if is_str_num(value):
                self.obj = self.obj.withColumn(key, F.lit(value))
            elif is_list_of_str_num(value):
                new_obj = DataFrame(obj=value, columns=key).obj
                self.obj = self.obj.withColumn("row_idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
                new_obj = new_obj.withColumn("row_idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))

                self.obj = self.obj.join(new_obj, self.obj.row_idx == new_obj.row_idx).drop("row_idx")

        raise Exception("key format not supported")

    def join(self, new_obj, on=None, how='inner', inplace=False):

        assert isinstance(new_obj, DataFrame)

        if on is not None:
            if inplace:
                self.obj = self.obj.join(new_obj.obj, on=on, how=how)
            else:
                return self.obj.join(new_obj.obj, on=on, how=how)
        else:
            if inplace:
                self.obj = self.obj.withColumn("row_idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
                new_obj1 = new_obj.obj.withColumn("row_idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))

                self.obj = self.obj.join(new_obj1, self.obj.row_idx == new_obj1.row_idx).drop("row_idx")
            else:
                obj1 = self.obj.withColumn("row_idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
                new_obj1 = new_obj.obj.withColumn("row_idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))

                return obj1.join(new_obj1, obj1.row_idx == new_obj1.row_idx).drop("row_idx")


    def apply(self, func, schema=None, column=None):
        if column is not None:
            assert isinstance(column, str)
        cols = self.obj.columns
        col = ('_c1' if column is None else column)
        new_udf = F.udf(lambda x: new_func(x, cols, func),(Type.StringType() if schema is None else schema))
        return self.obj.withColumn(col, 
            new_udf(F.struct([F.col(i) for i in cols]))).select(col)
