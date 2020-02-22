import os
import sys
import logging
from datetime import datetime, timedelta
from collections import namedtuple

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, IntegerType, LongType

try:
    import configparser
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    print("Running in local mode")
except ModuleNotFoundError:
    print("Running in cluster mode")

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

# set up a utility named tuple to be used integrity checks between dimension tables and foreign keys in fact table
#    i94_field - original fact table field
#    check_field - added field to validate original field, 
#    new_field - replacement field that becomes a foregn key to a dimenstion table
#    default_value - to be filled instead of Null values
#    field_desc - foregn key/ dim table description
#    is-date - flags date field vs. non-date fields
DimTableParms = namedtuple('DimTableParms', ['dim_table', 'i94_field', 'check_field', 'new_field', 'default_value', 'field_desc', 'is_date'])

class DimTableChecks:
    def __init__(self, dim_tbl_id):
        self.dim_table = dim_tbl_id
        self.null_before = 0
        self.incorrect = 0
        self.null_after = 0
        
class FactTableChecks:
    def __init__(self, fact_tbl_id):
        self.fact_table = fact_tbl_id
        self.rec_read = 0
        self.rec_saved = 0

        
class I94DataProcessing:    
    def __init__(self):
        self.dim_table_dict = {}
        self.dim_table_checks = {}
        self.fact_table_checks = {}
        self._sas_40y = (datetime(2060, 1, 1) - datetime(1960, 1, 1)).days
        self._sas_today = (datetime.today() - datetime(1960, 1, 1)).days

    def start_spark(self):    
        self.spark = SparkSession.builder.\
            config("spark.jars.packages",'org.apache.hadoop:hadoop-aws:2.7.5,saurfang:spark-sas7bdat:2.0.0-s_2.11')\
            .enableHiveSupport().getOrCreate()

    def read_fact_data(self, input_path: str, input_file: str):
        """ Read fact table data
            Insert selected fields into fact table
        """
        self._fact_tbl_name = input_file
        full_path = os.path.join(input_path, input_file) 
        logging.info('Reading fact table, {} ...'.format(full_path))
        if input_file[-3:] == 'csv':
            self.df_i94rec = self.spark.read.csv(full_path, header = True)  
        else:
            self.df_i94rec = self.spark.read.format('com.github.saurfang.sas.spark').load(full_path)
        
        fact_len = self.df_i94rec.count()
        logging.info('Read {} rows of fact table, {}'.format(fact_len, input_file))
        if input_file not in self.fact_table_checks:
            self.fact_table_checks[input_file] = FactTableChecks(input_file)
        self.fact_table_checks[input_file].rec_read = fact_len
        #self.df_i94rec.printSchema()

        self.df_i94rec = self.df_i94rec.select(self.df_i94rec['cicid'].cast(IntegerType()), 
                                               self.df_i94rec['i94yr'].cast(IntegerType()), 
                                               self.df_i94rec['i94mon'].cast(IntegerType()), 
                                               self.df_i94rec['arrdate'].cast(IntegerType()), 
                                               self.df_i94rec['depdate'].cast(IntegerType()), 
                                               self.df_i94rec['i94res'].cast(IntegerType()), 
                                               'i94port', 
                                               self.df_i94rec['i94mode'].cast(IntegerType()), 
                                               'i94addr', 
                                               self.df_i94rec['i94visa'].cast(IntegerType()),  
                                               'visatype', 
                                               self.df_i94rec['i94bir'].cast(IntegerType()), 
                                               self.df_i94rec['biryear'].cast(IntegerType()), 
                                               'gender', 'matflag', 'dtaddto', 
                                               self.df_i94rec['insnum'].cast(IntegerType()), 
                                               self.df_i94rec['admnum'].cast(LongType()))
        #self.df_i94rec.show(2)

    def read_dim_data(self, input_path: str):
        """ Read data for all dimension tables
            Create a DimTableParms object for each dimension table
            Add this object to dimension table dictionary
            
            :param input_path: location of dimension tables
        """
        logging.info('Reading input data for dimension tables...')
        states = self.spark.read.csv(input_path + "states.csv", header=True)
        countries = self.spark.read.csv(input_path + "countries.csv", header=True)
        arrival_modes = self.spark.read.csv(input_path + "arrival_modes.csv", header=True)
        visa_categories = self.spark.read.csv(input_path + "visa_categories.csv", header=True)
        visa_types = self.spark.read.csv(input_path + "visa_types.csv", header=True)
                               
        # combine 2 sources for airports/ports of entry
        airports_web = self.spark.read.csv(input_path + "airport-codes_csv.csv", header=True)
        airports_i94 = self.spark.read.csv(input_path + "airpots_provided.csv", header=True)
        ports = self._combine_airports(airports_web, airports_i94)
                               
        self.dim_table_dict['addr'] = DimTableParms(states, 'i94addr', 'addr_id', 'addr_id', '99', 'First Intended Address', False)
        self.dim_table_dict['resid'] = DimTableParms(countries, 'i94res', 'country_id', 'country_id', '999', 'Country of Residence', False)
        self.dim_table_dict['arrival_modes'] = DimTableParms(arrival_modes, 'i94mode', 'arr_mode_id', 'arr_mode_id', 9, 'Arrival Mode', False)
        self.dim_table_dict['visa_categories'] = DimTableParms(visa_categories, 'i94visa', 'visacategory_id', 'visacategory_id', 9, 'Visa Category', False)
        self.dim_table_dict['visa_types'] = DimTableParms(visa_types, 'visatype', 'visatype_id', 'visatype_id', '888', 'Visa Type', False)
        self.dim_table_dict['ports'] = DimTableParms(ports, 'i94port', 'port_id', 'port_id', '888', 'Port of Entry', False)
        self.dim_table_dict['arrival_date'] = DimTableParms(None, 'arrdate', 'check_arr_date', 'arr_date', None, 'Arrival Date', True)
        self.dim_table_dict['depart_date'] =  DimTableParms(None, 'depdate', 'check_dep_date', 'dep_date', None, 'Departure Date', True)

        
    def clean_primary_key_dim(self):
        """ Drop duplicates and remove rows with primary key = Null for each newly read dimenstion table
        """
        logging.info('Checking primary key in dimension tables for NULL values ...')
        for tbl_name, dim_tbl_obj in self.dim_table_dict.items():   
            if dim_tbl_obj:
                dim_table = dim_tbl_obj.dim_table
                if dim_table:
                    prim_key = dim_tbl_obj.new_field

                    dim_table.dropDuplicates()
                    null_count = dim_table.filter(dim_table[prim_key].isNull()).count()
                    msg = "'{}' dim table: {} = '{}' has {} NULL values".format(tbl_name, 'Primary Key', prim_key, null_count)
                    if null_count > 0:
                        logging.warning(msg + ",  removing NULL values...")
                        dim_table.dropna(subset = prim_key)
                    else:
                        logging.info(msg)
                        

    def clean_primary_key_fact(self):
        """ Drop duplicates and remove rows with primary key = Null in fact table
        """
        self.df_i94rec.dropDuplicates()
        prim_key = 'cicid'
        null_count = self.df_i94rec.filter(self.df_i94rec[prim_key].isNull()).count()
        msg = "{} fact table: {} = '{}' has {} NULL values".format(self._fact_tbl_name, 'Primary Key', prim_key, null_count)
        if null_count > 0:
            logging.warning(msg + ",  removing NULL values...")
            self.df_i94rec.dropna(subset = prim_key)
        else:
            logging.info(msg)
            

    def count_nulls_fact(self, before: bool):
        """ 
            Count how many values are missing for a foreign key column or a date column in fact table
            This function is called twice:  before and after data validation/conversion
        """
        logging.info("Checking for NULL in foreign key columns of fact table {} data processing...". \
                     format('before' if before else 'after'))
        for tbl_name, dim_tbl_obj in self.dim_table_dict.items():
            
            field_name = dim_tbl_obj.i94_field if before else dim_tbl_obj.new_field
            
            null_count = self.df_i94rec.filter(self.df_i94rec[field_name].isNull()).count()
            logging.info("'{}' = {} has {} NULL values".format(dim_tbl_obj.field_desc, field_name, null_count))
            
            if tbl_name not in self.dim_table_checks:
                self.dim_table_checks[tbl_name] = DimTableChecks(dim_tbl_obj.new_field)
            if before:
                self.dim_table_checks[tbl_name].null_before = null_count
            else:
                self.dim_table_checks[tbl_name].null_after = null_count
        
        
    def add_check_field(self):
        """ 
            Checking correctness of values in foreign key columns of fact table
            Add an extra column with validation results for each foreign key 
        """
        logging.info("Checking correctness of values in foreign key columns of fact table...")
        for key, dim_tbl_obj in self.dim_table_dict.items():
            if dim_tbl_obj.is_date:
                if 'arrival' in key:
                   sas_limit = self._sas_today 
                else:
                   sas_limit = self._sas_40y 
                self._check_sas_dates(dim_tbl_obj, sas_limit)
            else:
                self._add_check_field(dim_tbl_obj)

                
    def count_incorrect(self, before = True):
        """ count how many values in a fact table column are filled incorrectly
        """
        logging.info("Counting incorrect values in foreign key columns of fact table...")
        print(datetime.now())
        for tbl_name, dim_tbl_obj in self.dim_table_dict.items():
            
            # if a value was not found in the dimension table then it is incorrect and 'new_field' = NULL
            incorrect_values = self.df_i94rec.filter(self.df_i94rec[dim_tbl_obj.check_field].isNull()).select(dim_tbl_obj.i94_field)
            incorrect_values_count = incorrect_values.count()
            logging.info("'{}' is incorrect: {}".format(dim_tbl_obj.field_desc, incorrect_values_count))
            if incorrect_values_count > 0:
                logging.info("Sample of '{}' is incorrect: {}".format(dim_tbl_obj.field_desc, [list(row) for row in incorrect_values.take(10)]))
                
            # add number of rows with incorrect data to summary dictionary
            if tbl_name not in self.dim_table_checks:
                self.dim_table_checks[tbl_name] = DimTableChecks(dim_tbl_obj.new_field)
            self.dim_table_checks[tbl_name].incorrect = incorrect_values_count
            print(datetime.now())
        
    
    def convert_data(self):
        """ Fix incorrect values in foreign key columns of fact table
        """
        logging.info("Fixing incorrect values in foreign key columns of fact table...")
        for tbl_name, dim_tbl_obj in self.dim_table_dict.items():
            if dim_tbl_obj.is_date:
                self._convert_sas_dates(dim_tbl_obj, ('arrival' in tbl_name))
            else:
                self._fix_incorrect(tbl_name, dim_tbl_obj)
        #self.df_i94rec.show(2)
        
        
    def drop_original_cols(self):
        """ drop the original 'i94_field' for all columns that were reviewed 
            and replaced with validated/converted columns 
        """
        for dim_tbl_obj in self.dim_table_dict.values():
            self.df_i94rec = self.df_i94rec.drop(dim_tbl_obj.i94_field)
            if dim_tbl_obj.is_date:
                self.df_i94rec = self.df_i94rec.drop(dim_tbl_obj.check_field)
        #self.df_i94rec.show(2)

        
    def _check_sas_dates(self, dim_tbl_obj: DimTableParms, sas_limit:int):
        """
            Validate SAS dates in i94arr and i94dep fields against date limits
            Add an extra column with validation results
        """
        def check_sas_date(sas_date_limit: int):
            def base_check(sas_date):
                if sas_date is not None and sas_date >= 0 and sas_date <= sas_date_limit:
                    return 1
                elif sas_date is not None:
                    return None 
                else:  
                    return -1 
            return base_check
        
        check_date_udf = F.udf(check_sas_date(sas_limit), IntegerType())

        self.df_i94rec = self.df_i94rec.withColumn(dim_tbl_obj.check_field, 
                                                   check_date_udf(self.df_i94rec[dim_tbl_obj.i94_field]))
                               

    def _convert_sas_dates(self, dim_tbl_obj: DimTableParms, arr_or_dep: bool):
        """ Convert SAS date in i94arr and i94dep fields into regular date format
            arrival date:   if check_col = 1 then convert
                            if check_col = None then def-value    
                            if check_col = -1 then def-value    
            departure date: if check_col = 1 then convert
                            if check_col = None then None    
                            if check_col = -1 then None    
        """
        logging.info('Converting sas date format for {} ...'.format(dim_tbl_obj.new_field))
        
        def convert_sas_date(def_value: bool):
            def convert(sas_date, i94yr, i94mon, check_col):
                if check_col == 1:
                    return datetime(1960, 1, 1) + timedelta(days=sas_date)
                elif def_value:
                    return datetime(i94yr, i94mon, 1) 
                else:
                    return None
            return convert

        logging.info('Converting {} ...'.format(dim_tbl_obj.new_field))
        convert_date_udf = F.udf(convert_sas_date(arr_or_dep), DateType())

        self.df_i94rec = self.df_i94rec.withColumn(dim_tbl_obj.new_field, 
                                                   convert_date_udf(self.df_i94rec[dim_tbl_obj.i94_field], 
                                                                    self.df_i94rec.i94yr, 
                                                                    self.df_i94rec.i94mon, 
                                                                    self.df_i94rec[dim_tbl_obj.check_field]))

                               
    def _add_check_field(self, dim_tbl_obj: DimTableParms): 
        """
            create a new field for each foreign key column to check for incorrect values
            verify that IDs in a fact table column match ids in a corresponding dimenstion table:
             - replace ID = Null with a default value
             - find incorrect IDs and replace them with a default value
        """
        # replace NULL with 'default_value' 
        self.df_i94rec = self.df_i94rec.fillna(dim_tbl_obj.default_value, [dim_tbl_obj.i94_field])

        # check for incorrect codes: join with corresponding dimension table
        self.df_i94rec = self.df_i94rec.join(dim_tbl_obj.dim_table.select(dim_tbl_obj.new_field), 
                                             self.df_i94rec[dim_tbl_obj.i94_field]==dim_tbl_obj.dim_table[dim_tbl_obj.new_field], 
                                             'left')

    
    def _fix_incorrect(self, tbl_name: str, dim_tbl_obj: DimTableParms): 
        """ fix incorrect values for a foreign key field
        """
        # if any incorrect values were found then replace them with a default value
        if tbl_name in self.dim_table_checks and self.dim_table_checks[tbl_name].incorrect > 0:
            logging.info('Fixing {} ...'.format(dim_tbl_obj.new_field))
            self.df_i94rec = self.df_i94rec.fillna(dim_tbl_obj.default_value, [dim_tbl_obj.new_field])
            

    def _combine_airports(self, airports: DataFrame, airports_i94: DataFrame):
        """ 
            generate Ports of Entry dimenstion table using 2 data sources:
            1. a dataset provided along with immigration data. It contains 91 unidentified ports
            2. a dataset downloaded from https://datahub.io/core/airport-codes#data
        """
        logging.info("Merging 2 datasets for Ports of entry... ") 
        # clean out airport codes
        airports_i94 = airports_i94.dropna(subset = ['location_ir94']).dropna(subset = ['port_id'])

        airports = airports.select('type', 'name', 'municipality', 'iso_region', 'iata_code').dropDuplicates()
        # filter out other types of landing like heliports
        airports = airports.filter(airports['type'].contains('airport'))
        # clean out airport codes
        airports = airports.dropna(subset = ['iata_code']).filter(airports['iata_code'] != '-').filter(airports['iata_code'] != '0')
        # combine location details 
        airports = airports.withColumn('location_web', F.concat_ws(', ', airports['name'], airports['municipality'], airports['iso_region']))
        # keep only required columns: port_id and location
        airports = airports.select(airports['iata_code'].alias('port_id'), airports['location_web'])
        # remove duplicate airports by ranking and taking only rank = 1
        window = Window.partitionBy("port_id").orderBy(F.desc('location_web')).rowsBetween(Window.unboundedPreceding, Window.currentRow)
        airports = airports.dropDuplicates().withColumn('airport_rank', F.rank().over(window))
        airports = airports.where('airport_rank == 1').drop('airport_rank')
        # print('After processing, airports count: ', airports.count())
        # airports.show(n=2)

        # combine provided set of ports with a larger set of airport codes
        # if a port_id exists in both sets then replace i94 names that contains "Collapsed" and "No PORT" names with existing names
        # otherwise, use i94 name
        combined = airports_i94.join(airports, 'port_id', 'outer')
        combined = combined.withColumn('location', 
                                       F.when(combined['location_web'].isNull(), combined['location_ir94']). 
                                       when(combined['location_ir94'].isNull(), combined['location_web']). 
                                       when(combined['location_ir94'].contains('Collapsed'), combined['location_web']). 
                                       when(combined['location_ir94'].contains('No PORT'), combined['location_web']). 
                                       otherwise(combined['location_ir94']))
        combined = combined.drop('location_web', 'location_ir94')

        logging.info("Ports of entry dimenstion table contains {} records with {} of Null port_id". 
                      format(combined.count(), combined.filter(combined.port_id.isNull()).count()))

        return combined


    def create_time_table(self, output_path: str):
        """
            Extract distinct arrival dates from fact table 
            Add these dates to 'dates' dimenstion table
        """
        logging.info('Creating "dates" dimension table using fact table...')
        # get unique arrival dates
        date_tbl = self.df_i94rec.select('arr_date').dropDuplicates()
        # parse this date and add year, month, day fields
        date_tbl = date_tbl.withColumn('year', F.year('arr_date')).withColumn('month', F.month('arr_date')). \
                            withColumn('day', F.dayofmonth('arr_date')) 
            
        logging.info('Saving "dates" dimension table...')
        date_tbl.write.partitionBy('year', 'month').json(os.path.join(output_path, 'dates'), mode = 'append')
    
    
    def check_duplicates(self):
        """ check for duplicate primary key in dimension tables 
        """
        for tbl_name, dim_tbl_obj in self.dim_table_dict.items():
            if dim_tbl_obj.dim_table is not None:
                logging.info("Checking for duplicate primary key in dimension table '{}' ...".format(tbl_name))
                prim_key = dim_tbl_obj.new_field
                dim_tbl_obj.dim_table.createOrReplaceTempView(tbl_name)
                
                sql_query = """select count({}) as total, count(distinct {}) as uniq from {}""".format(prim_key, prim_key, tbl_name)
                res = self.spark.sql(sql_query).collect()
                
                if res and len(res) > 0:
                    res_dict = res[0].asDict()
                    msg = "Primary Key counts in dimension table '{}': total = {}, unique = {}.". \
                                        format(tbl_name, res_dict['total'], res_dict['uniq'])
                    if res_dict['total'] != res_dict['uniq']:
                        logging.warning(msg + " There are duplicates!")
                    else:
                        logging.info(msg + " There are NO duplicates.")
                else:                            
                    logging.warning("Dimension table '{}' is empty".format(tbl_name))
                
    def save_fact_data(self, output_path: str):
        """ save fact table to json file partitioned by year and month
        """
        logging.info('Saving fact table {} ...'.format(self._fact_tbl_name))
        self.df_i94rec.write.partitionBy('i94yr', 'i94mon').json(os.path.join(output_path, 'i94records'), mode = 'append')
        
        fact_len = self.df_i94rec.count()
        logging.info('Saved {} rows of fact table, {}'.format(fact_len, self._fact_tbl_name))
        if self._fact_tbl_name not in self.fact_table_checks:
            self.fact_table_checks[self._fact_tbl_name] = FactTableChecks(self._fact_tbl_name)
        self.fact_table_checks[self._fact_tbl_name].rec_saved = fact_len

        
    def save_dim_data(self, output_path: str):
        """ save all dimension tables to json files 
        """
        for tbl_name, dim_tbl_obj in self.dim_table_dict.items():
            if dim_tbl_obj.dim_table:
                print('Saving dimension table {} ...'.format(dim_tbl_obj.dim_table))
                
                dim_mode = 'append' if dim_tbl_obj.is_date else 'overwrite'
                dim_tbl_obj.dim_table.write.json(os.path.join(output_path, tbl_name), mode = dim_mode)
        
        
    def print_col_check_dict(self):
        """ print integrity checks statitstics for foreign keys in fact table
        """
        logging.info("Field name, NULLs before, Incorrect before, NULLs after")
        for field_name, value in self.dim_table_checks.items():
            logging.info("'{}': {}, {}, {}". \
                         format(field_name, value.null_before, value.incorrect, value.null_after))
            
        
    def print_fact_check_dict(self):
        """ print integrity checks for fact table 
        """
        logging.info("Fact tbl name, Records read, Records saved")
        for field_name, value in self.fact_table_checks.items():
            logging.info("'{}': {}, {}". \
                         format(field_name, value.rec_read, value.rec_saved))


def run_etl(i94_obj: I94DataProcessing, spark: SparkSession, 
            input_files: list, fact_input_path: str, dim_input_path: str, output_path: str):
    """ run ETL process for dimension and fact tables
    """
    ## process all dimension tables except 'dates' table
    i94_obj.read_dim_data(dim_input_path)
    i94_obj.clean_primary_key_dim()
    i94_obj.check_duplicates()
    i94_obj.save_dim_data(output_path)
    
    ## process fact table and 'dates' dimension table that is derived from fact table        
    for fact_input_file in input_files:
        i94_obj.read_fact_data(fact_input_path, fact_input_file)    

        # clean up primary key in fact
        i94_obj.clean_primary_key_fact()
        # clean up foreign keys in fact table
        i94_obj.count_nulls_fact(before = True)
        i94_obj.add_check_field()
        i94_obj.count_incorrect()
        
        # align foreign keys in fact table with the primary keys in dimension tables
        i94_obj.convert_data()
        i94_obj.drop_original_cols()
        
        # validate fact table after fixing
        i94_obj.count_nulls_fact(before = False)
        i94_obj.create_time_table(output_path)
        i94_obj.print_col_check_dict()
        i94_obj.save_fact_data(output_path)
        
    i94_obj.print_fact_check_dict()
            
def main():
    # set up input and output folders
    if len(sys.argv) != 2:
        print("Please specify a parameter: 'test' or 'onefile' or 'allfiles'")
        sys.exit()

    if sys.argv[1] == 'test':
        fact_input_path = './fact_data' 
        input_files = [file for file in os.listdir(fact_input_path) if len(file) and file[0] != '.']
        output_path = './output_data/'
        
    elif sys.argv[1] == "onefile":
        fact_input_path = '../../data/18-83510-I94-Data-2016/'
        input_files = ['i94_aug16_sub.sas7bdat']
        #### add S3 bucket and folder here
        output_path = 's3a://de-projects/capstone/output_data/'
        
    elif sys.argv[1] == "allfiles":
        fact_input_path = '../../data/18-83510-I94-Data-2016/'
        input_files = [file for file in os.listdir(fact_input_path) if len(file) and file[0] != '.']
        #### add S3 bucket and folder here
        output_path = 's3a://de-projects/capstone/output_data/'
    else:
        print("The only accepted arguments are 'test' or 'onefile' or 'allfiles'")
        sys.exit()
        
    print(input_files)
        
    dim_input_path = './dim_data/'
    
    # start Spark and run ETL
    i94_obj = I94DataProcessing()
    spark = i94_obj.start_spark()
    run_etl(i94_obj, spark, input_files, fact_input_path, dim_input_path, output_path)
                               
if __name__ == "__main__":
    main()