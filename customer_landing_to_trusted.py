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
AmazonS3_node1703781227557 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-stedi-human-balance/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1703781227557",
)

# Script generated for node SQL Query
SqlQuery2232 = """
select * from myDataSource
where shareWithResearchAsOfDate != 0;
"""
SQLQuery_node1703781261316 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2232,
    mapping={"myDataSource": AmazonS3_node1703781227557},
    transformation_ctx="SQLQuery_node1703781261316",
)

# Script generated for node Amazon S3
AmazonS3_node1703781391713 = glueContext.getSink(
    path="s3://project-stedi-human-balance/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703781391713",
)
AmazonS3_node1703781391713.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
AmazonS3_node1703781391713.setFormat("json")
AmazonS3_node1703781391713.writeFrame(SQLQuery_node1703781261316)
job.commit()
