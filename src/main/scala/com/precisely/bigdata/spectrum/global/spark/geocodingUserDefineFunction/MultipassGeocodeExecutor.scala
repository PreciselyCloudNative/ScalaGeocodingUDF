package com.precisely.bigdata.spectrum.global.spark.geocodingUserDefineFunction

import java.util

import com.mapinfo.geocode.{GeocodeAddress, GeocoderPreferences}
import com.mapinfo.geocode.api.{Candidate, StandardMatchMode}
import org.apache.commons.collections.CollectionUtils
/*
 * A custom GeocodeExecutor that contains the logic to perform a multipass geocode
 */
class MultipassGeocodeExecutor() extends com.pb.bigdata.geocoding.api.GeocodeExecutor {


  import com.mapinfo.geocode.api.{Address, GeocodeType, GeocodingAPI, Preferences, Response}
  def isAcceptableMatch(response: Response): Boolean = {
    return response.getCandidates.size() > 0 && response.getCandidates.get(0).getCustomFieldValue("MATCH_SCORE").toInt > 80
  }
  @throws(classOf[com.mapinfo.geocode.GeocodingException])
  @Override
  def call(address: Address, preferences: Preferences, geocoder: GeocodingAPI): Response = {
    var response: Response = null
    var geocodePass: String = ""
    // Multi Line responses
    val customPreferences = new GeocoderPreferences(preferences)
    customPreferences.setMatchMode(StandardMatchMode.STANDARD)
    customPreferences.setFallbackToGeographic(true)
    customPreferences.setFallbackToPostal(true)
    geocodePass = "multi line"
    response = geocoder.geocode(GeocodeType.ADDRESS, address, customPreferences)
    val multiPassResponse: Response = response
      if(!isAcceptableMatch(response)){
      // build single line address
      val singleLine = address.getMainAddressLine + " " + address.getAreaName3 + " " + address.getPostCode1
      val singleLineAddress = new GeocodeAddress()
      singleLineAddress.setMainAddressLine(singleLine)
      singleLineAddress.setCountry(address.getCountry)
      geocodePass = "single line"
      response = geocoder.geocode(GeocodeType.ADDRESS, singleLineAddress, customPreferences)
      val singlePassResponse = response
      if(!isAcceptableMatch(response)) {
        if(singlePassResponse.getCandidates.get(0).getCustomFieldValue("MATCH_SCORE").toInt >
          multiPassResponse.getCandidates.get(0).getCustomFieldValue("MATCH_SCORE").toInt){
          response = singlePassResponse
        }
        else if(singlePassResponse.getCandidates.get(0).getCustomFieldValue("MATCH_SCORE").toInt <
          multiPassResponse.getCandidates.get(0).getCustomFieldValue("MATCH_SCORE").toInt){
          response = multiPassResponse
        }
        else if(singlePassResponse.getCandidates.get(0).getCustomFieldValue("MATCH_SCORE").toInt ==
          multiPassResponse.getCandidates.get(0).getCustomFieldValue("MATCH_SCORE").toInt){

        }
      }
      }
    response.getCandidates.get(0).getAddress.addCustomField("PASS_TYPE",geocodePass)
    return response
    }
  }
