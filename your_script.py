from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark
spark = SparkSession.builder.appName("Referral_Final_Pipeline").getOrCreate()

data_path = "data/"
output_path = "data/Output"

# Function to load and clean CSVs
def load_and_clean(file):
    df = spark.read.csv(data_path + file, header=True, inferSchema=True)
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType):
            df = df.withColumn(f.name, trim(col(f.name)))
    df = df.na.replace(
        ["", "null", "NULL", "None", "NaN", "nan"],
        [None]*6
    )
    return df

# Load all data
lead = load_and_clean("lead_log.csv")
ur   = load_and_clean("user_referrals.csv")
url  = load_and_clean("user_referral_logs.csv")
ul   = load_and_clean("user_logs.csv")
urs  = load_and_clean("user_referral_statuses.csv")
rr   = load_and_clean("referral_rewards.csv")
pt   = load_and_clean("paid_transactions.csv")

# Convert timestamps safely
lead = lead.withColumn("lead_created_local",
                       from_utc_timestamp(to_timestamp("created_at"), coalesce("timezone_location", lit("Asia/Jakarta"))))

ur = ur.withColumn("referral_at_local", from_utc_timestamp(to_timestamp("referral_at"), lit("Asia/Jakarta"))) \
       .withColumn("updated_at_local", from_utc_timestamp(to_timestamp("updated_at"), lit("Asia/Jakarta")))

url = url.withColumn("url_created_local", from_utc_timestamp(to_timestamp("created_at"), lit("Asia/Jakarta")))

ul = ul.withColumn("membership_expired_local",
                   from_utc_timestamp(to_timestamp("membership_expired_date"), coalesce("timezone_homeclub", lit("Asia/Jakarta"))))

pt = pt.withColumn("transaction_at_local",
                   from_utc_timestamp(to_timestamp("transaction_at"), coalesce("timezone_transaction", lit("Asia/Jakarta"))))

# Join user_referrals with user_referral_logs
df1 = ur.alias("ur").join(url.alias("url"), col("ur.referral_id") == col("url.user_referral_id"), "left") \
    .select(
        col("ur.referral_id"),
        col("ur.referral_at"),
        col("ur.referral_at_local"),
        col("ur.referee_id"),
        col("ur.referee_name"),
        col("ur.referee_phone"),
        col("ur.referral_reward_id"),
        col("ur.referral_source"),
        col("ur.referrer_id"),
        col("ur.transaction_id"),
        col("ur.updated_at"),
        col("ur.updated_at_local"),
        col("ur.user_referral_status_id"),
        col("url.is_reward_granted").alias("reward_granted")
    )

# Join with user_referral_statuses
df2 = df1.join(urs.alias("urs"), df1.user_referral_status_id == col("urs.id"), "left") \
         .select(df1["*"], col("urs.description").alias("referral_status"))

# Join with referral_rewards
df3 = df2.join(rr.alias("rr"), df2.referral_reward_id == col("rr.id"), "left") \
         .select(df2["*"], col("rr.reward_value"), col("rr.created_at").alias("reward_created_at"))

# Join with paid_transactions
df4 = df3.join(pt.alias("pt"), df3.transaction_id == col("pt.transaction_id"), "left") \
         .select(df3["*"], col("pt.transaction_status"), col("pt.transaction_at_local"),
                 col("pt.transaction_location"), col("pt.transaction_type"))

# Join with user_logs
df5 = df4.join(ul.alias("ul"), df4.referrer_id == col("ul.user_id"), "left") \
         .select(df4["*"], col("ul.name").alias("referrer_name"),
                 col("ul.phone_number").alias("referrer_phone_number"),
                 col("ul.homeclub").alias("referrer_homeclub"),
                 col("ul.membership_expired_local"),
                 col("ul.is_deleted").alias("referrer_is_deleted"))

# Join with lead_log
df6 = df5.join(lead.alias("lead"), df5.referee_id == col("lead.lead_id"), "left") \
         .select(df5["*"], col("lead.source_category").alias("lead_source_category"))

# Derive referral_source_category
df7 = df6.withColumn(
    "referral_source_category",
    when(col("referral_source") == "User Sign Up", "Online")
    .when(col("referral_source") == "Draft Transaction", "Offline")
    .when(col("referral_source") == "Lead", col("lead_source_category"))
    .otherwise(None)
)

# Safe timestamp columns
df7 = df7.withColumn("transaction_at_ts", to_timestamp("transaction_at_local")) \
         .withColumn("referral_at_ts", to_timestamp("referral_at_local")) \
         .withColumn("membership_expired_ts", to_timestamp("membership_expired_local"))

# Safe month extraction using try_cast
df7 = df7.withColumn("transaction_month", expr("try_cast(month(transaction_at_ts) as int)")) \
         .withColumn("referral_month", expr("try_cast(month(referral_at_ts) as int)"))

# Business logic validation (fully null-safe)
df8 = df7.withColumn(
    "is_business_logic_valid",
    when(
        (col("reward_value") > 0) &
        (lower(col("referral_status")) == "berhasil") &
        col("transaction_id").isNotNull() &
        (upper(col("transaction_status")) == "PAID") &
        (upper(col("transaction_type")) == "NEW") &
        col("transaction_at_ts").isNotNull() &
        col("referral_at_ts").isNotNull() &
        (col("transaction_at_ts") > col("referral_at_ts")) &
        col("membership_expired_ts").isNotNull() &
        (col("membership_expired_ts") > col("transaction_at_ts")) &
        (~col("referrer_is_deleted")) &
        (col("reward_granted") == True) &
        (col("transaction_month") == col("referral_month")),
        True
    )
    .when(
        (col("referral_status").isin("Menunggu", "Tidak Berhasil")) &
        col("reward_value").isNull(),
        True
    )
    .otherwise(False)
)

# Capitalize string columns except homeclub
for c, t in df8.dtypes:
    if t == "string" and "homeclub" not in c.lower():
        df8 = df8.withColumn(c, initcap(col(c)))

# Select final columns and drop duplicates
final_df = df8.select(
    monotonically_increasing_id().alias("referral_details_id"),
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at_local",
    "referrer_id",
    "referrer_name",
    "referrer_phone_number",
    "referrer_homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "referral_status",
    "reward_value",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "reward_created_at",
    "is_business_logic_valid"
).dropDuplicates(["referral_id"])

# Write final CSV
final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print("FINAL REFERRAL REPORT GENERATED")
