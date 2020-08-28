![Precisely](__sitelogo__precisely.png)

Spectrum Geocoding for Big Data(Multipass Sample)
---------------------
This sample for the Spark Geocoding API in Scala demonstrates how to improve geocoding results(both forward and reverse) by performing multipass geocoding

## Description
The sample loads a file with parse address into a Spark DataFrame and geocode the address with geocoding attributes using 'Spectrum Geocoding for Big Data' SDK and Reference Data

### Forward multipass geocoding
1. During the first pass includes individual address fields will be submitted to the geocoder with Standard User specified match mode. Fallback to
Postal & Geography will be on. The reason for turning on fallback for the first pass is because multi-line will give back less false positives for both
streets and postal\geographic matches.
2. If Match Score < 80 OR Match Type != Address from first pass, then only the second pass below will be triggered otherwise candidate from first
pass will be picked up.
3. The second pass will submit single line address (with concatenated address fields) to the geocoder. Fallback to Postal & Geography will be
on. The match mode in second pass will be match mode preference setup by the user while submitting the job.
4. If Match Score > 80 and Match type Address from second pass, we pick the output candidate from second pass.
5. But if Match Score < 80 OR match type Address from second pass, we pick the output from first pass or second pass depending on:
6. Which candidate has better Match score.
7. If Match Scores are equal then candidate will be picked depending on which pass has better Match Type
8. If Match Type is also equal then candidate will be picked depending on which pass has better location type or location score.


## Instructions
Follow the instructions to use Geocoding SDK and Reference Data

### Geocoding SDK
Spectrum Geocoding for Big Data is an external library, that will enable your sample to use Precisely Geocoding API, deployable in your Hadoop system

* **spectrum-bigdata-geocoding-sdk-spark2-<version>.jar**
To run this sample locally, you should have this library under "lib" directory of this sample.
To run this sample on cluster, this library must be on Master Node

### Geocoding Reference Data
This sample uses geocoding reference data provided by Precisely [Data Experience](https://data.precisely.com). Geocoding attributes will be generated using this data

* **CAN-EGM-MASTERLOCATION-CA8-105-yyyymm-GEOCODING.spd**
* **EGM-WORLD-AP-WLD-105-yyyymm-GEOCODING.spd**
* **EGM-WORLD-STREET-WBL-105-yyyymm-GEOCODING.spd**
* **GBR-EGM-ORDNANCESURVEY-GB7-105-yyyymm-GEOCODING.spd**
* **IDN-EGM-NAVTEQ-ID2-105-yyyymm-GEOCODING.spd**
* **KLDmmyyyy.spd**
* **KNTmmyyyy.spd**
* **PHL-EGM-TOMTOM-PH1-105-yyyymm-GEOCODING.spd**
* **POL-EGM-NAVTEQ-PL2-105-yyyymm-GEOCODING.spd**
* **TUR-EGM-NAVTEQ-AP-TR4-105-yyyymm-GEOCODING.spd**
* **TUR-EGM-NAVTEQ-STREET-TR2-105-yyyymm-GEOCODING.spd**
* **VNM-EGM-NAVTEQ-VN2-105-yyyymm-GEOCODING.spd**

If you haven't installed the reference data in your cloud environment, follow the steps to [install data](https://support.pb.com/help/hadoop/landingpage/docs/geocoding/spectrum-big-data-geocoding-v4-0-0-hortonworks-install-guide.pdf)

### Build Sample Code

The sample includes a gradle build system around it.  To build the sample code, use the following command from the root of the sample:

    gradlew clean build

To run this sample on a cluster, you should copy this build(/build/libs/geocoding-4.0.0.7-all.jar) to the Master Node

## Running the sample on a cluster
* Number of data nodes: 4
* Instance Type: m5a.2xlarge

To execute the sample on a cluster, you must have a download directory in every node of your cluster(/pb/downloads) with all read and write permissions. This directory is used to locally cache the reference data for use during the search.

### Spark Submit in Command Prompt:
```
spark-submit --class com.precisely.bigdata.spectrum.global.spark.MultipassGeocoding --master yarn \
--deploy-mode cluster --executor-memory 20G --executor-cores 6 \
--conf spark.yarn.maxAppAttempts=1 --conf spark.hadoop.fs.s3a.access.key=**<S3 ACCESS KEY>** \
--conf spark.hadoop.fs.s3a.secret.key=**<S3 SECRET KEY>** \
--jars /pb/geocoding/software/spark2/custom/geocoding-4.0.0.7-all.jar \
/pb/geocoding/software/spark2/sdk/lib/spectrum-bigdata-geocoding-sdk-spark2-4.0.0.7.jar \
--input s3a://INPUT/ADDRESS/FILE/LOCATION \
--output s3a://OUTPUT/FILE/LOCATION \
--format psv --num-partitions=36 --download-location /pb/downloads \
--geocoding-resources-location hdfs:///pb/geocoding/software/resources/ \
--geocoding-preferences-filepath hdfs:///pb/geocoding/software/resources/config/geocodePreferences.xml \
--geocoding-output-fields X Y formattedStreetAddress formattedLocationAddress precisionCode PB_KEY areaName3 areaName1 postCode1 \
--geocoding-input-fields mainAddressLine=1 areaName3=3 areaName1=4 postCode1=5 country=6 
```

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

