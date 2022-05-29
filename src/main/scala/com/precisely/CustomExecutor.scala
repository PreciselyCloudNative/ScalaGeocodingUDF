package com.precisely

import java.util.Collections
import com.precisely.addressing.v1.{Addressing, LookupType, Preferences}
import com.precisely.addressing.v1.model.{FactoryDescriptionBuilder, KeyValue, PreferencesBuilder, RequestAddress, Response}
import com.precisely.bigdata.addressing.spark.api.{AddressingExecutor, RequestInput}
import org.apache.commons.collections.CollectionUtils

import scala.collection.JavaConverters._

/*
 * A custom Executor that contains the logic to perform verify and then multipass geocode
 */

class CustomExecutor extends AddressingExecutor {
  val verifyPreferences: Preferences = new PreferencesBuilder()
    .withReturnAllInfo(true)
    .withFactoryDescription(new FactoryDescriptionBuilder().withLabel("finalist").build)
    .withCustomPreference("VM_DATA_BLOCK", "N")
    .withCustomPreference("EARLY_WARNING_SYSTEM", "true")
    .withCustomPreference("R777_DELIVERABLE", "true")
    .withCustomPreference("REMOVE_NOISE_CHARS", "true")
    .withCustomPreference("RETURN_ALIAS_STREET_WITH_TYPE", "ON")
    .withCustomPreference("RETURN_INPUT_FIRM", "true")
    .withCustomPreference("RETURN_SLK_SECONDARY", "B")
    .withCustomPreference("STAND_ALONE_PMB", "N")
    .withCustomPreference("STAND_ALONE_UNIT", "N")
    .withCustomPreference("FINALIST_LOG_LEVEL", "2")
    .withCustomPreference("SCORE_THRESHOLD", "0")
    //    .withCustomPreference("DPV_FILE_MODE", "FLT")
    //    .withCustomPreference("FIND_LACSLINK", "true")
    //    .withCustomPreference("FIND_DPV", "true")
    //    .withCustomPreference("FIND_RDI", "true")
    .build

  // set preference to return all fields
  val geocoderPreferences: Preferences = new PreferencesBuilder()
    .withReturnAllInfo(true)
    //    .withMatchMode("CASS")
    .withCustomPreference("FIND_STREET_CENTROID", "false")
    .withCustomPreference("FIND_DPV", "true")
    .withCustomPreference("FIND_LACSLINK", "true")
    .withCustomPreference("FIND_RDI", "true")
    .build

  // set preference to return all fields and Match Mode to Relaxed
  val relaxedGeocoderPreferences: Preferences = new PreferencesBuilder()
    .withReturnAllInfo(true)
    .withMatchMode("RELAXED")
    .withCustomPreference("FIND_DPV", "true")
    .withCustomPreference("FIND_LACSLINK", "true")
    .withCustomPreference("FIND_RDI", "true")
    .build

  // set preference to return all fields
  val cassPreferences: Preferences = new PreferencesBuilder()
    .withReturnAllInfo(true)
    .withMatchMode("CASS")
    .withCustomPreference("FIND_STREET_CENTROID", "false")
    .withCustomPreference("FIND_DPV", "true")
    .withCustomPreference("FIND_LACSLINK", "true")
    .withCustomPreference("FIND_RDI", "true")
    .build


  var response: Response = null
  var geocodePass: String = ""

  override def execute(input: RequestInput, preferences: Option[Preferences], addressing: Addressing): Response = {

    val verifyResponse = addressing.verify(input.requestAddress(), verifyPreferences)

    var multiLineRequest = input.requestAddress()

    var singleLine = input.requestAddress().getAddressLines.get(0) + " " +
      input.requestAddress().getCity + " " +
      input.requestAddress().getAdmin1 + " " +
      input.requestAddress().getPostalCode + " " + input.requestAddress().getCountry

    var response: Response = null
    var geocodePass: String = ""
    if (CollectionUtils.isNotEmpty(verifyResponse.getResults)) {

      val verifyResults = verifyResponse.getResults.get(0)
      val verifyAddress = verifyResults.getAddress

      //  create multiLine request from verify Response
      multiLineRequest = new RequestAddress()
      multiLineRequest.setAddressLines(List(verifyResults.getAddressLines.get(0)).asJava)
      //       multiLineRequest.setAddressNumber(verifyAddress.getAddressNumber)
      multiLineRequest.setAdmin1(verifyAddress.getAdmin1.getLongName)
      multiLineRequest.setAdmin2(verifyAddress.getAdmin2.getLongName)
      multiLineRequest.setCity(verifyAddress.getCity.getLongName)
      multiLineRequest.setPostalCode(verifyAddress.getPostalCode)
      multiLineRequest.setCountry(verifyAddress.getCountry.getIsoAlpha3Code)

      // create singleline request from verify Response
      singleLine = verifyResults.getAddressLines.get(0) + " " + verifyResults.getAddressLines.get(1) + " " + verifyAddress.getCountry.getIsoAlpha3Code

    }

    response = addressing.geocode(multiLineRequest, geocoderPreferences)
    geocodePass = "multi line"

    if (!isAddressLevelMatch(response) || response.getResults.get(0).getCustomFields.get("PB_KEY") == null) {
      val singleLineRequest: RequestAddress = new RequestAddress()
      singleLineRequest.setAddressLines(Collections.singletonList(singleLine))
      singleLineRequest.setCountry(input.requestAddress().getCountry)

      val multiLineResponse = response
      response = addressing.geocode(singleLineRequest, geocoderPreferences)
      geocodePass = "single line"
      val singleLineResponse = response
      if (!isAddressLevelMatch(singleLineResponse) || singleLineResponse.getResults.get(0).getCustomFields.get("PB_KEY") == null) {

        if (multiLineResponse.getResults.get(0).getCustomFields.get("PB_KEY") != null &&
          singleLineResponse.getResults.get(0).getCustomFields.get("PB_KEY") == null) {
          response = multiLineResponse
          geocodePass = "multi line PBKEY"
        }
        else if (multiLineResponse.getResults.get(0).getCustomFields.get("PB_KEY") == null &&
          singleLineResponse.getResults.get(0).getCustomFields.get("PB_KEY") != null) {
          response = singleLineResponse
          geocodePass = "single line PBKEY"
        }
        else if (multiLineResponse.getResults.get(0).getCustomFields.get("PB_KEY") != null &&
          singleLineResponse.getResults.get(0).getCustomFields.get("PB_KEY") != null) {
          if (multiLineResponse.getResults.get(0).getScore > singleLineResponse.getResults.get(0).getScore) {
            response = multiLineResponse
            geocodePass = "multi line has better score"
          }
          else {
            response = singleLineResponse
            geocodePass = "single line has better score"
          }
        }
        else if (multiLineResponse.getResults.get(0).getCustomFields.get("PB_KEY") == null &&
          singleLineResponse.getResults.get(0).getCustomFields.get("PB_KEY") == null) {
          if (!isAddressLevelMatch(singleLineResponse)) {
            response = addressing.geocode(singleLineRequest, relaxedGeocoderPreferences)
            geocodePass = "single line relaxed"
            if (CollectionUtils.isNotEmpty(response.getResults) && CollectionUtils.isNotEmpty(multiLineResponse.getResults) &&
              multiLineResponse.getResults.get(0).getScore > response.getResults.get(0).getScore) {
              response = multiLineResponse
              geocodePass = "multi line has better score"
            }
            else if (response == null) {
              response = multiLineResponse
              geocodePass = "multi line as single line is null"
            }
          }
        }
      }
    }
    val hashMap = finalistFields(verifyResponse)

    response.getResults.get(0).getCustomFields.putAll(hashMap.asJava)
    response.getResults.get(0).getCustomFields.put("Geocode_Pass", geocodePass)
    return response
    //    return verifyResponse
  }

  //
  def isAddressLevelMatch(response: Response): Boolean = {
    if (response.getResults.size() > 0 && "ADDRESS".equals(response.getResults.get(0).getExplanation.getAddressMatch.getType.label)
      && response.getResults.get(0).getScore >= 90) {
      return true
    }
    false
  }

  def finalistFields(response: Response): Predef.Map[String, String] = {

    val verifyResult = response.getResults.get(0)
    val hashMap = Predef.Map(
      "std_source" -> verifyResult.getExplanation.getSource.get("label"),
      "std_confidence" -> verifyResult.getScore.toString,
      "std_formattedStreetAddress" -> verifyResult.getAddressLines.get(0),
      "std_formattedLocationAddress" -> verifyResult.getAddressLines.get(1),
      "std_housenumber" -> verifyResult.getCustomFields.get("HOUSENUMBER"),
      "std_predirectional" -> verifyResult.getCustomFields.get("LEADINGDIRECTIONAL"),
      "std_streetname" -> verifyResult.getCustomFields.get("STREETNAME"),
      "std_streettype" -> verifyResult.getCustomFields.get("STREETTYPE"),
      "std_postdirectional" -> verifyResult.getCustomFields.get("TRAILINGDIRECTIONAL"),
      "std_apartmentlabel" -> verifyResult.getCustomFields.get("APARTMENTLABEL"),
      "std_apartmentnumber" -> verifyResult.getCustomFields.get("APARTMENTNUMBER"),
      "std_city" -> verifyResult.getCustomFields.get("PREFERREDCITYNAME"),
      "std_stateprovince" -> verifyResult.getCustomFields.get("PREFERREDSTATE"),
      "std_postalcode_base" -> verifyResult.getAddress.getPostalCode,
      "std_postalcode_addon" -> verifyResult.getAddress.getPostalCodeExt,
      "std_country" -> verifyResult.getAddress.getCountry.getIsoAlpha3Code,
      "std_countyname" -> verifyResult.getAddress.getAdmin2.getLongName,
      "std_city_short_name" -> verifyResult.getAddress.getCity.getShortName,
      "std_city_state_record_name" -> verifyResult.getAddress.getCity.getLongName,
      "std_status" -> verifyResult.getCustomFields.get("USA.STATUS"),
      "std_status_code" -> verifyResult.getCustomFields.get("STATUS.CODE"),
      "std_status_description" -> verifyResult.getCustomFields.get("USA.STATUS.DESCRIPTION"),
      "std_usaltaddr" -> verifyResult.getCustomFields.get("ALTSTREET"),
      "std_county_fips" -> verifyResult.getCustomFields.get("FIPSCOUNTYNUMBER"),
      "std_finalist_recordtype" -> verifyResult.getCustomFields.get("MATCHLEVEL"),
      "std_congressional_dist" -> verifyResult.getCustomFields.get("CONGRESSIONALDISTRICT"),
      "std_altstreet" -> verifyResult.getCustomFields.get("ALTSTREET")
//      "std_rdi_retcode" -> verifyResult.getCustomFields.get("RDI_RETCODE"),
//      "std_dpv" -> verifyResult.getCustomFields.get("DPV_CONFIRM"),
//      "std_dpvnostat" -> verifyResult.getCustomFields.get("DPV_NO_STAT"),
//      "std_cmra" -> verifyResult.getCustomFields.get("DPV_CMRA"),
//      "std_dpvfootnote1" -> verifyResult.getCustomFields.get("DPV_FOOTNOTE1"),
//      "std_dpvfootnote2" -> verifyResult.getCustomFields.get("DPV_FOOTNOTE2"),
//      "std_dpvfootnote3" -> verifyResult.getCustomFields.get("DPV_FOOTNOTE3")

    )
    return hashMap
  }

  override def execute(lookupType: LookupType, preferences: Option[Preferences], addressing: Addressing, keyValues: KeyValue*): Response = ???

  override def execute(x: Double, y: Double, country: String, preferences: Option[Preferences], addressing: Addressing): Response = ???
}
