# Pyspark Geocoding UDF

Precisely Spectrum Geocoding for Big Data is a toolkit for processing enterprise data for large-scale spatial analysis. Billions of records in a single file can be processed in parallel, using Hive and Apache Spark's cluster processing framework, yielding results faster than ever. Unlike traditional processing techniques that used to take weeks to process the data, now the data processing can be done in a few hours using this product.

 The following user-defined function (UDF) are designed to support Precisely's Geocoding functions in PySpark. These functions are written and compiled in programming language Scala and meant to be described in PySpark.

## Geocoding Variables

| Name | Description  | Example |
|---|---| ---- |
| resourcesLocationLocal  | Location of the geocoding resources directory which contains the configurations and libraries needed for geocoding. <br><br> <b>Note:</b> If using a remote path, e.g. HDFS or S3, then set pb.download.location. Local paths must be present on all nodes that tasks will run on. | ```hdfs:///precisely/geocoding/software/resources/``` |
| dataLocationLocal | File path(s) to one or more geocoding datasets. A path may be a single dataset (extracted or an unextracted SPD), or a directory of datasets. Multiple paths must be separated with a comma. <br><br> <b>Note:</b> If using a remote path, e.g. HDFS or S3, then set pb.download.location. Local paths must be present on all nodes that tasks will run on. | ```hdfs:///precisely/geo_addr/data/``` |
| extractLocationLocal | File path to where the geocoding datasets will be extracted.  | ```/precisely/geo_addr/data/extractionDirectory``` |
| [outputFields](#commonly-used-output-fields) | Comma-separated list of fields requested from the geocoder. You must either set this variable. <br><br><b>Refer [outputFields](#commonly-used-output-fields) for more information</b>  | ```X,Y,formattedStreetAddress,formattedLocationAddress``` |
| [operation](#operation) | Name of addressing operation you want to perform. You must either set this variable. <br><br><b>Refer [operation](#operation) for more information</b>  | ```Geocode```,```multipass```,```verify```,```lookup```,```reverseGeocode``` |

## Operation

| Name | Description  | Example |
|---|---|---|
| Geocode  |  Accepts an address as input and returns the standardized US or international address and additional attribution, including the location coordinates, for the address. <br> <b> More information [here](https://docs.precisely.com/docs/sftw/hadoop/landingpage/docs/geocoding/webhelp/Geocoding/source/geocoding/geocodefunc_spark_geocode.html)| ```operation = "geocode"```|
| Reverse Geocode | Accepts a location (coordinates and coordinate system) as input and returns the standardized address and additional attribution for that location. <br> <b> More information [here](https://docs.precisely.com/docs/sftw/hadoop/landingpage/docs/geocoding/webhelp/Geocoding/source/geocoding/geocodefunc_spark_revgeocode.html)| ```operation = "reverseGeocode"```|
| Verify  | Accepts an address as input and returns the standardized US or international address and additional attribution for the address.  | ```operation = "verify"```|
| Lookup | Accepts unique key for an address and returns a geocoded matched candidate. Supported keys come from USA or AUS GNAF data (for example, P0000GL638OL for USA data and GAACT715000223 for AUS) and are of types PB_KEY or GNAF_PID.  |```operation = "lookup"```|
| Multipass:  | Improves geocoding results by performing verify first and then geocoding. With this multipass addressing example, for all results without address level precision, a second geocoding pass is run using single line input address, which may increase match rate.  | ```operation = "multipass"```|

## Input Fields

The following table lists commonly used fields available for specifying an address.

|  Parameter | Type | Description  |
|---|---| --- |
| addressLines[n]  | String | The address lines, as needed, to support the standard address format of a given country (where n is an integer starting at 0). To specify a single line address, set addressLines[0].|
| placeName  | String | The building name, place name, point of interest (POI), company or firm name associated with the input address.|
| building  | String | The building name.|
| addressNumber  | String | The address number information.|
| street | String | The street name.|
| floor | String | The floor number.|
| room | String | The room number.|
| unit | String | The unit number, such as "3B".|
| unitType | String | The unit type, such as APT, STE, FLAT, etc.|
| neighborhood | String | The city subdivision or locality.|
| suburb | String | The sublocality.|
| borough | String | The borough.|
| city | String | The city or town name.|
| admin2 | String | The secondary geographic area, typically a county or district.|
| admin1 | String | The largest geographic area, typically a state or province.|
| postalCode | String | The postal code in the appropriate format for the country.|
| postalCodeExt | String | The postal code extension in the appropriate format for the country.|
| country | String |The ISO 3166-1 country code (Alpha-2, Alpha-3, or numerical), or a common name of the country, such as United States of America. Required when data for more than one country is configured.|
| x | String | The X coordinate (longitude) of the address. Required for Reverse Geocoding.|
| y | String | The Y coordinate (latitude) of the address. Required for Reverse Geocoding.|
| key | String | The unique identifier for the address. Required for lookup.|
| type | String | The type of key for a lookup. The default value is PB_KEY.|

## Commonly Used Output Fields

Comma-separated list of fields requested from the geocoder. You must either set this variable or set the preferences in the UDF query.

|  Parameter | Description  |
|---|---|
| score  |  A number from 0-100 that indicates how much the input was changed in order to match to the output candidate.|
| addressLines[n] | Return the specified line of the formatted address.|
| address.placeName  | The building name, place name, Point of Interest (POI), company or firm name associated with the input address.|
| address.addressNumber  | The address number.  |
| address.street  | The street name.  |
| address.unit  | The unit number, such as "3B".  |
| address.unitType  | The unit type, such as APT, STE, FLAT, etc.  |
| address.admin1.longName  | The full name of the largest geographic area, typically a state, province, or region.  |
| address.city.longName  | The full name of the city or town.  |
| address.postalCode  | The postal code in the appropriate format for the country.  |
| address.postalCodeExt  | The postal code extension in the appropriate format for the country.  |
| address.country.name  | The country name.  |
|address.country.isoAlpha3Code|The ISO 3166-1 Alpha-3 country code.|
|address.formattedAddress|The formatted address as a single line.|
|address.formattedStreetAddress|The formatted street address line.|
|address.formattedLocationAddress|The formatted last address line.|
|location.feature.geometry.x|The X coordinate (longitude) of the address.|
|location.feature.geometry.y|The Y coordinate (latitude) of the address.|
|customFields['PB_KEY']|The PreciselyID unique identifier that is returned when an address match is made using the Master Location Dataset (MLD). |
|customFields['PRECISION_CODE']|A code describing the precision of the geocode.|
|customFields['MATCH_TYPE']||
|customFields['LOC_CODE']|Location codes indicate the accuracy of the assigned geocode.|
|customFields['MATCH_CODE']|	Match codes indicate the portions of the address that matched or did not match to the reference file.|
|customFields['DPV_CONFIRM']|	Indicates if a match occurred for DPV data.|
|customFields['Geocode_Pass']|Indicate type of geocoding pass|

#### <b>For more output fields [here](https://docs.precisely.com/docs/sftw/hadoop/landingpage/docs/geocoding/webhelp/Geocoding/source/geocoding/addressing/addressing_output_fields.html)</b>

#### Aliasing:
This feature allows you to specify a simpler or more meaningful name for the returned output field. To specify an alias, insert the word "as" between the output field name and the desired alias name. For aliases that contain characters other than letters or numbers, single quotes are required around the alias.
<!-- For example: ```customField['PB_KEY'] as 'Precisely ID'``` -->

#### Example For Output Fields with aliasing:
```
outputFields = ["score",
           "explanation.source['label'] as label",
           "location.feature.geometry.coordinates.x as LON",
           "location.feature.geometry.coordinates.y as LAT",
           "address.formattedAddress as FullAddress",
           "address.formattedStreetAddress as StreetAddress",
           "address.formattedLocationAddress as LocationAddress",
           "address.placeName as PlaceName",
           "address.addressNumber as AddressNumber",
           "address.street as Street",
           "address.unit as Unit",
           "address.unitType as UnitType",
           "address.admin1.longName as State",
           "address.city.longName as City",
           "address.postalCode as PostalCode",
           "address.postalCodeExt as PostalCodeExt",
           "address.country.name as CountryName",
           "address.country.isoAlpha3Code as Country",
           "customFields['PRECISION_CODE'] as PrecisionCode",
           "customFields['MATCH_TYPE'] as MatchType",
           "customFields['LOC_CODE'] as LocationCode",
           "customFields['MATCH_CODE'] as MatchCode",
           "customFields['PB_KEY'] as PBKEY",
           "customFields['DPV_CONFIRM'] as DpvConfirm",
           "customFields['Geocode_Pass'] as GeocodePass"]
```


# Example of Addressing UDF

``` 
operation = "geocode"
resourcesLocationLocal = "/precisely/sdk/resources"
dataLocationLocal = "/precisely/data"
sqlContext = SQLContext(spark.sparkContext)
outputFields = ["score",
           "explanation.source['label'] as label",
           "location.feature.geometry.coordinates.x as LON",
           "location.feature.geometry.coordinates.y as LAT",
           "address.formattedAddress as FullAddress"]

DF = DataFrame(spark.sparkContext._jvm.com.precisely.Addressing.addressingDF(
    inputDF._jdf, operation, resourcesLocationLocal, extractLocationLocal, dataLocationLocal, outputFields, inputFields), 
    sqlContext)
```
