package com.precisely

import com.precisely.addressing.v1.model._
import com.precisely.addressing.v1.{Addressing, LookupType, Preferences}
import com.precisely.bigdata.addressing.spark.api.{AddressingExecutor, RequestInput}
import org.apache.commons.collections.CollectionUtils

import java.util.Collections
import scala.collection.JavaConverters._

/*
 * A custom Executor that contains the logic to perform verify and then multipass geocode
 */

class CustomUnitExecutor extends AddressingExecutor {
  val verifyPreferences: Preferences = new PreferencesBuilder()
    .withReturnAllInfo(true)
    .withFactoryDescription(new FactoryDescriptionBuilder().withLabel("ggs").build)
    .withCustomPreference("FIND_LACSLINK", "true")
    .withCustomPreference("FIND_DPV", "true")
    .withCustomPreference("FIND_RDI", "true")
    .build

  // set preference to return all fields
  val geocoderPreferences: Preferences = new PreferencesBuilder()
    .withReturnAllInfo(true)
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


  var response: Response = null
  var verifiedUnit: String = "false"

  override def execute(input: RequestInput, preferences: Option[Preferences], addressing: Addressing): Response = {

    var verifyResponse = addressing.verify(input.requestAddress(), verifyPreferences)

    var multiLineRequest = input.requestAddress()

    var singleLine = input.requestAddress().getAddressLines.get(0) + " " +
      input.requestAddress().getCity + " " +
      input.requestAddress().getAdmin1 + " " +
      input.requestAddress().getPostalCode + " " + input.requestAddress().getCountry

    if (!isAddressLevelMatch(verifyResponse)) {
      val singleLineRequest: RequestAddress = new RequestAddress()
      singleLineRequest.setAddressLines(Collections.singletonList(singleLine))
      singleLineRequest.setCountry(input.requestAddress().getCountry)
      verifyResponse = addressing.verify(singleLineRequest, verifyPreferences)
    }

    if (CollectionUtils.isNotEmpty(verifyResponse.getResults)) {

      val verifyResults = verifyResponse.getResults.get(0)
      val verifyAddress = verifyResults.getAddress
      val verifyCustom = verifyResults.getCustomFields

      if (verifyCustom.get("MAIL_STOP") != null || verifyCustom.get("ADDR2") != null) {

        val AddressLine2 = if (verifyCustom.get("MAIL_STOP") != null) " UNIT " + verifyCustom.get("MAIL_STOP")
        else " UNIT " + verifyCustom.get("ADDR2")

        //  create multiLine request from verify Response
        multiLineRequest = new RequestAddress()
        multiLineRequest.setAddressLines(List(verifyResults.getAddressLines.get(0) + AddressLine2).asJava)
        //       multiLineRequest.setAddressNumber(verifyAddress.getAddressNumber)
        multiLineRequest.setAdmin1(verifyAddress.getAdmin1.getLongName)
        multiLineRequest.setAdmin2(verifyAddress.getAdmin2.getLongName)
        multiLineRequest.setCity(verifyAddress.getCity.getLongName)
        multiLineRequest.setPostalCode(verifyAddress.getPostalCode)
        multiLineRequest.setCountry(verifyAddress.getCountry.getIsoAlpha3Code)

        // create singleline request from verify Response
        singleLine = verifyResults.getAddressLines.get(0) + AddressLine2 + " " + verifyResults.getAddressLines.get(1) + " " + verifyAddress.getCountry.getIsoAlpha3Code
      }

      response = addressing.geocode(multiLineRequest, geocoderPreferences)

      if (isUnitMatch(response)) {
        verifiedUnit = "true"
      }
      else {
        val singleLineRequest: RequestAddress = new RequestAddress()
        singleLineRequest.setAddressLines(Collections.singletonList(singleLine))
        singleLineRequest.setCountry(input.requestAddress().getCountry)

        response = addressing.geocode(singleLineRequest, geocoderPreferences)
        if (isUnitMatch(response)) {
          verifiedUnit = "true"
        }
        else {
          verifiedUnit = "false"
        }
      }
    }
    response.getResults.get(0).getCustomFields.put("VERIFIED_UNIT", verifiedUnit)
    response
  }

  //
  def isUnitMatch(response: Response): Boolean = {
    if (response.getResults.size() > 0 && "Y".equals(response.getResults.get(0).getCustomFields().get("DPV_CONFIRM"))
      && response.getResults.get(0).getCustomFields().get("HIUNIT") != null) {
      return true
    }
    false
  }

  def isAddressLevelMatch(response: Response): Boolean = {
    if (response.getResults.size() > 0 && "ADDRESS".equals(response.getResults.get(0).getExplanation.getAddressMatch.getType.label)) {
      return true
    }
    false
  }

  override def execute(lookupType: LookupType, preferences: Option[Preferences], addressing: Addressing, keyValues: KeyValue*): Response = ???

  override def execute(x: Double, y: Double, country: String, preferences: Option[Preferences], addressing: Addressing): Response = ???
}
