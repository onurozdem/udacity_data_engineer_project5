# Project: Data Pipelines with Airflow
This repository was created as part of my training on Udacity. For detailed information, you can check the https://www.udacity.com/course/data-engineer-nanodegree--nd027 link.

## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Datasets
This project working with two datasets that reside in S3. Here are the S3 links for each:

Song data: s3://udacity-dend/song_data/
Log data: s3://udacity-dend/log-data/

## Operators
In project scope, the following operators have been customized

### Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

### Fact and Dimension Operators
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

### Data Quality Operator
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

## ETL Pipeline
In this project, used Airflow for orchestration etl processes. Airflow DAG architecture is below. 

  > ![p1](pics/example-dag.png)
  
 
# Preparation for Airflow
1. To go to the Airflow UI

2. Click on the Admin tab and select Connections.
  > ![p2](pics/admin-connections.png)

3. Under Connections, select Create.
  > ![p3](pics/create-connection.png)

4. On the create connection page, enter the following values:
   * **Conn Id**: Enter `aws_credentials`.
   * **Conn Type**: Enter `Amazon Web Services`.
   * **Login**: Enter your Access key ID from the IAM User credentials you downloaded earlier.
   * **Password**: Enter your Secret access key from the IAM User credentials you downloaded earlier.
   Once you've entered these values, select Save and Add Another.

  > ![p4](pics/connection-aws-credentials.png)
 4. On the next create connection page, enter the following values:

   * **Conn Id**: Enter `redshift`.
   * **Conn Type**: Enter `Postgres`.
   * **Host**: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
   * **Schema**: Enter `dev`. This is the Redshift database you want to connect to.
   * **Login**: Enter `awsuser`.
   * **Password**: Enter the password you created when launching your Redshift cluster.
   * **Port**: Enter `5439`.
    Once you've entered these values, select Save.
  > ![p5](pics/cluster-details.png)
  > ![p6](pics/connection-redshift.png)
    Awesome! You're now all configured to run Airflow with Redshift.

## Project Running
When you copy all files in this project to under your Airflow working directory, Airflow automatically will discover dag. After your code changes, you can switch on dag. 
 

