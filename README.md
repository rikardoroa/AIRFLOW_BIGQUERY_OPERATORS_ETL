## ENSITECH CHALLENGE - BIGQUERY ETL OPERATIONS

## Main Considerations

* 1 ->  A GOOGLE Cloud Account , with the Service Account Connections Keys (Json File from GCP)

* 2 ->  Google Cloud Providers Libraries, to install those libraries use **__pip install apache-airflow[google]__**

## Recommendations

* 1 -> for this case scenario I use csv file for small data, but for upload large amount of data in Bigquery is necessary use avro files for **__best practices__**

 

## Previous considerations
 * 1 - > for this challenge I use my own json connection files, for security you must use your own **__(previous configuration in GCP)__**, I won't share my connections key for security.

 * 2 - > config the right write and read permissions in the console
 
 * 3 -> **__config properly the project and project id in google console__** for this challenge, I use  my own configurations

 * 4 -> **__I will submit my .env file for configuration only__**, if you want to use another projects or variables(bucket,tables, etc), you are welcome to do so.

 
## Project description

* 1 -> Bigquery transformations using several Bigquery libraries and operator for Bucket object creation in Google Cloud Storage and also dataset and tables creation in Bigquery

* 2 -> the Dag run every hour, you can config the Dag to run differently

## enjoy!
