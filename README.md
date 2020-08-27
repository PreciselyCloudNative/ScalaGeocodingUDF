![Precisely](__sitelogo__precisely.png)

Spectrum Geocoding for Big Data(Multipass Sample)
---------------------
This sample for the Spark Geocoding API in Scala demonstrates how to improve geocoding results(both forward and reverse) by performing multipass geocoding

## Description
The sample loads a file with parse address into a Spark DataFrame and geocode the address with geocoding attributes using 'Spectrum Geocoding for Big Data' SDK and Reference Data

### Forward multipass geocoding
Pass qualifier is result with a PreciselyID and suboptimal precision levels, i.e., any result code between S5-S8 (street interpolated to roof toop).
First pass is running using Multi Line address, a second pass is running using single line address matching, third and fourth passes are running using relaxed matchmode and fallback to nearest PreciselyID respectively. Max Fallback Search distance is 5280 feet

### Reverse multipass geocoding
Pass qualifier is result with a PreciselyID. First pass is running using location with default search distance, a second pass is running using location with custom search distance. Max Search distance is 5280 feet

## Instructions
Follow the instructions to use Geocoding SDK and Reference Data

### Geocoding SDK
Spectrum Geocoding for Big Data is an external library, that will enable your sample to use Precisely Geocoding API, deployable in your Hadoop system

* **spectrum-bigdata-geocoding-sdk-spark2-<version>.jar**
To run this sample locally, you should have this library under "lib" directory of this sample.
To run this sample on EMR, this library can be in Amazon S3 or on Master Node

### Geocoding Reference Data
This sample uses geocoding reference data provided by Precisely [Data Experience](https://data.precisely.com). Geocoding attributes will be generated using this data

* **US Master Location Data(KLDmmyyyy.spd)**
* **US NAVTEQ Streets (KNTmmyyyy.spd)**

If you haven't installed the reference data in your cloud environment, follow the steps to [install data](https://support.pb.com/help/hadoop/landingpage/docs/geocoding/spectrum-big-data-geocoding-v4-0-0-hortonworks-install-guide.pdf)

### Build Sample Code

The sample includes a gradle build system around it.  To build the sample code, use the following command from the root of the sample:

    gradlew clean build

To run this sample on EMR, you should copy this build(/build/libs/geocoding-4.0.0.7-all.jar) to Amazon S3 or to Master Node

## Running the sample on a cluster
* Number of data nodes: 5
* Instance Type: m5a.2xlarge

To execute the sample on a cluster, you must have a download directory in every node of your cluster(/mnt/pb/downloads) with all read and write permissions. This directory is used to locally cache the reference data for use during the search.
You can set this in your bootstrapping scripts

### Spark Submit in Command Prompt:
```
spark-submit --class com.precisely.bigdata.spectrum.global.spark.MultipassGeocoding --master yarn --deploy-mode cluster \
--executor-memory 10G --executor-cores 4 \
--conf spark.yarn.maxAppAttempts=1 \
--jars s3://LOCATION/OF/GEOCODING/SDK/spectrum-bigdata-geocoding-sdk-spark2-<version>.jar s3://LOCATION/OF/YOUR/SAMPLE/BUILD/geocoding-4.0.0.7-all.jar \
--input s3://LOCATION/OF/YOUR/INPUT/FILE/address_file.csv --format csv \
--num-partitions=36 \
--geocoding-resources-location /mnt/pb/geocoding/software/resources/ \
--geocoding-preferences-filepath /mnt/pb/geocoding/software/resources/config/geocodePreferences.xml \
--output s3://LOCATION/OF/OUTPUT/DIRECTORY/output_address_file
```

### Spark Submit As an EMR Step: 
1.	‘JAR location’: command-runner.jar
2.	‘Arguments’: spark-submit --class com.precisely.bigdata.spectrum.global.spark.MultipassGeocoding --master yarn --deploy-mode cluster --executor-memory 10G --executor-cores 4 --conf spark.yarn.maxAppAttempts=1 --jars s3://LOCATION/OF/GEOCODING/SDK/spectrum-bigdata-geocoding-sdk-spark2-<version>.jar s3://LOCATION/OF/YOUR/SAMPLE/BUILD/geocoding-4.0.0.7-all.jar --input s3://LOCATION/OF/YOUR/INPUT/FILE/address_file.csv --format csv --num-partitions=36 --geocoding-resources-location /mnt/pb/geocoding/software/resources/ --geocoding-preferences-filepath /mnt/pb/geocoding/software/resources/config/geocodePreferences.xml --output s3://LOCATION/OF/OUTPUT/DIRECTORY/output_address_file 

### Optional Parameters:
1. --download-location		: Location of the directory where reference data will be downloaded to. This path must exist on every data node. Default is /mnt/pb/downloads 
2. --geocoding-output-fields: The requested geocode fields to be included in the output. Default fields are "X", "Y", "formattedStreetAddress", "formattedLocationAddress", "PrecisionCode", "PB_KEY", "areaName3", "areaName1", "postCode1","PASS_TYPE" 
3. --fallback 				: Fallback search distance(in feet) to nearest PreciselyID and this will enable Multipass geocoding class. This works only with forward geocoding
4. --search-distance		: Increase the search distance(in feet) to find an address match and this will enable Multipass reverse geocoding class. This works only with reverse geocoding

#### Format supported:
1. csv
2. tsv
3. psv
4. parquet
5. orc
	  
## Hadoop libraries
Included with the sample are the Hadoop libraries required for running the spark sample on Windows.  The libraries were 
acquired through https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1.  These libraries are referenced in 
the build.gradle file by adding the path to them as an environment variable to the included unit test.  These libraries 
are included in the sample in accordance to this license: https://github.com/steveloughran/winutils/blob/master/LICENSE

