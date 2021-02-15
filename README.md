# airflow_first_dag
First airflow DAG solution
#General notes:
Since this is the first time I was writing an Airflow DAG, it took me a bit to catchup and understand. As the suggested time to attempt the assignment was 1-2 hours, I did not get into unittest module as I did not want to provide a wrong impression of how much I can code in 1-2 hours. Below modules were written in an hour and it took me a few hours to get the DAG right.

# Modules:
Module to read from files and create output file - assignment_main_read.py
DAG - assignment_main_dag.py

# Decription:
assignment_main_read.py

This module reads the file passed by the DAG. I have created 2 classes for each source file and for every read creating objects for these classes. 
Data needed in the final file is read from the source objects and is taken in a Pandas dataframe.
This dataframe is written in the output file.

Currently both input and output files are taken from local file system.

Input files - 
file_path from DAG - ../drive/
file_path/input_source_1/data_20210124.json
file_path/input_source_2/engagement_20210124.csv

Note: Input file names are being passed in the DAG right now.

Output file - 
file_path/output_' + date + '.csv'

assignment_main_dag.py

Contains the call for the main read module. While designing this, in interest of time I have kept the 2 input classes in same module as well as the writing output part.
I realized this, but it was a bit late that I could have created 3 modules - 2 for reads and one for write instead, within the DAG itself. However, as I mentioned above, I did not want to cheat on time.

# Execute:
I used below commands to setup airflow docker in my local-
curl LfO 'http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/latest/docker-compose.yaml'

After downloading the image, I ran below commands to create new directories per the docker-compose.yaml values and setup airflow user and password to an arbitrary value for my localhost; and initialize the containers.

  mkdir ./dags ./logs ./plugins

  docker-compose up airflow-init

Followed by-

  docker-compose up

After the airflow localhost is up and running. go and check on any browser of your choice to see localhost:8080 is up. You will see a login page. Now it is time to check the user and password fom the docker-compose.yaml. If you used the same steps I have mentioned above then your user and password are 'airflow'.

The last thing you need to do is, place both modules -assignment_main_read.py and assignment_main_dag.py in your dags folder on your local machine; along with the input files.
The output is generated in the dags folder as well with naming convention - output_<date>.csv. Here the <date> is the date taken from input file names.

E.g. if the inputs files came in for 01/24 - as engagement_20210124.csv and data_20210124.json, the output file name will be - output_20210124.csv

This code is expandable for more than 2 inputs and executing more than once in a day. Currently I have used Pandas, and hence the code has the capability to handle around 2GB data.
