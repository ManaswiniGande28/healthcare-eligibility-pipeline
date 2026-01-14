import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, initcap, regexp_replace, to_date, lit, when

spark = SparkSession.builder.appName("EligibilityPipeline").getOrCreate()

def process_partner(file_path, config):
    df = spark.read.option("header", "true").option("sep", config["delimiter"]).csv(file_path)
    
    for partner_col, std_col in config["mapping"].items():
        df = df.withColumnRenamed(partner_col, std_col)
    
    df = df.withColumn("first_name", initcap(col("first_name"))) \
           .withColumn("last_name", initcap(col("last_name"))) \
           .withColumn("email", lower(col("email"))) \
           .withColumn("partner_code", lit(config["partner_code"]))
    
    df = df.withColumn("dob", 
        when(to_date(col("dob"), "MM/dd/yyyy").isNotNull(), to_date(col("dob"), "MM/dd/yyyy"))
        .otherwise(to_date(col("dob"), "yyyy-MM-dd")))
    
    clean_phone = regexp_replace(col("phone"), r"\D", "")
    df = df.withColumn("phone", 
        regexp_replace(clean_phone, r"(\d{3})(\d{3})(\d{4})", r"$1-$2-$3"))
    
    return df.select("external_id", "first_name", "last_name", "dob", "email", "phone", "partner_code")

configs = {
    "acme": {"delimiter": "|", "partner_code": "ACME", "mapping": {"MBI": "external_id", "FNAME": "first_name", "LNAME": "last_name", "DOB": "dob", "EMAIL": "email", "PHONE": "phone"}},
    "bettercare": {"delimiter": ",", "partner_code": "BETTER_CARE", "mapping": {"subscriber_id": "external_id", "first_name": "first_name", "last_name": "last_name", "date_of_birth": "dob", "email": "email", "phone": "phone"}}
}

acme_df = process_partner("acme.txt", configs["acme"])
bc_df = process_partner("bettercare.csv", configs["bettercare"])

final_df = acme_df.union(bc_df)
final_df.show()