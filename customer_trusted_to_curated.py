import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1703855271599 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-stedi-human-balance/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1703855271599",
)

# Script generated for node Amazon S3
AmazonS3_node1703855274129 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-stedi-human-balance/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1703855274129",
)

# Script generated for node SQL Query
SqlQuery0 = """
select distinct customerName, email, phone, birthDay, serialNumber, registrationDate, lastUpdateDate, shareWithResearchAsOfDate, shareWithPublicAsOfDate, shareWithFriendsAsOfDate from myDataSource join myDataSource2 
on myDataSource.email = myDataSource2.user;
"""
SQLQuery_node1703855454137 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "myDataSource": AmazonS3_node1703855274129,
        "myDataSource2": AmazonS3_node1703855271599,
    },
    transformation_ctx="SQLQuery_node1703855454137",
)

# Script generated for node Amazon S3
AmazonS3_node1703855554447 = glueContext.getSink(
    path="s3://project-stedi-human-balance/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703855554447",
)
AmazonS3_node1703855554447.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
AmazonS3_node1703855554447.setFormat("json")
AmazonS3_node1703855554447.writeFrame(SQLQuery_node1703855454137)
job.commit()
