package com.precisely.bigdata.spectrum.global.spark.mapMarker

import com.mapinfo.geocode.{GeocodeAddress, GeocodingException}
import com.mapinfo.geocode.api.Candidate
import org.apache.commons.collections.CollectionUtils
import com.mapinfo.geocode.api.{Address, GeocodeType, GeocodingAPI, Preferences, Response}
import org.apache.commons.lang3.StringUtils

import scala.util.Try

class MultipassGeocodeExecutor(accumulator: GeocodingAccumulator) extends com.pb.bigdata.geocoding.api.GeocodeExecutor {

  private val MATCH_SCORE = "MATCH_SCORE";

  def call(address: Address, preferences: Preferences, geocoder: GeocodingAPI): Response = {
    var response:Response = null;
    try{
    response = geocode(address, preferences, geocoder)
    if(CollectionUtils.isNotEmpty(response.getCandidates)){
      accumulator.add(GeocodingAccumulator.SUCCESS)
      val candidate = response.getCandidates.get(0)
      if(candidate != null && StringUtils.isNotBlank(candidate.getPrecisionCode)){
	if(candidate.getPrecisionCode.startsWith("S"))
	  accumulator.add(GeocodingAccumulator.PREMIUM)
	else
	  accumulator.add(GeocodingAccumulator.BASIC)
      }
    }
    else
      accumulator.add(GeocodingAccumulator.FAILURE)
    }catch{
      case ex : GeocodingException => {
        accumulator.add(GeocodingAccumulator.FAILURE)
        throw ex;
      }
      case ex : Exception => {
        ex.printStackTrace()
        accumulator.add(GeocodingAccumulator.FAILURE)
        throw ex;
      }
    }
    return response
  }

  @throws(classOf[com.mapinfo.geocode.GeocodingException])
  @Override
  def geocode(address: Address, preferences: Preferences, geocoder: GeocodingAPI): Response = {

    val country = address.getCountry
    if (StringUtils.isEmpty(country)) {
      address.setCountry("NotProvided")
    } else {
      address.setCountry(country.trim)
    }

    val firstPassResponse = geocoder.geocode(GeocodeType.ADDRESS, address, preferences)
    var candidates = firstPassResponse.getCandidates
    var candidateFromFirstPass: Candidate = null

    if (CollectionUtils.isNotEmpty(candidates)) {
      candidateFromFirstPass = candidates.get(0)
      if (isFirstPassMatch(candidateFromFirstPass)) {
        return firstPassResponse
      }
    }


    val singleLineAddress: GeocodeAddress = toSingleLineAddress(address)

    val singleLineResult = Try(geocoder.geocode(GeocodeType.ADDRESS, singleLineAddress, preferences))

    candidates = singleLineResult.get.getCandidates
    var candidateFromSecondPass: Candidate = null

    if (CollectionUtils.isNotEmpty(candidates)) {
      candidateFromSecondPass = candidates.get(0)
      if (isSecondPassMatch(candidateFromSecondPass)) {
        return singleLineResult.get
      }
    }

    if (candidateFromFirstPass == null) {
      return singleLineResult.get
    } else if (candidateFromSecondPass == null) {
      return firstPassResponse
    } else if (isNotAddressMatchOrMatchScoreLessThan80(candidateFromSecondPass)) {
      if (getMatchScore(candidateFromSecondPass) > getMatchScore(candidateFromFirstPass)) {
        return singleLineResult.get
      } else if (getMatchScore(candidateFromSecondPass) < getMatchScore(candidateFromFirstPass)) {
        return firstPassResponse
      } else {
        // check which candidate has a better match type
        val matchTypeScore = MatchType.withNameWithDefault(candidateFromFirstPass.getPrecisionCode).id -
          MatchType.withNameWithDefault(candidateFromSecondPass.getPrecisionCode).id
        if(matchTypeScore > 0){
          return firstPassResponse
        }else if(matchTypeScore < 0){
          return singleLineResult.get
        }else{
          // location type and location score filtering
          val locationTypeScore = LocationType.withNameWithDefault(candidateFromFirstPass.getPrecisionCode).id -
            LocationType.withNameWithDefault(candidateFromSecondPass.getPrecisionCode).id
          if(locationTypeScore < 0){
            return singleLineResult.get
          }
        }
      }
    }
    return firstPassResponse
  }

  private def isNotAddressMatchOrMatchScoreLessThan80(candidate: Candidate) = {
    (candidate.getPrecisionCode != null &&
      MatchType.withNameWithDefault(candidate.getPrecisionCode).id != 10) ||
      getMatchScore(candidate) < 80.0
  }

  private def isSecondPassMatch(candidate: Candidate) = {
    candidate.getPrecisionCode != null &&
      MatchType.withNameWithDefault(candidate.getPrecisionCode).id == 10 &&
      getMatchScore(candidate) > 80.0
  }

  private def toSingleLineAddress(address: Address) = {
    val singleLine = Array(address.getStreetName, address.getAreaName3, address.getAreaName1, address.getPostCode1)
      .mkString(" ")

    val singleLineAddress = new GeocodeAddress()
    singleLineAddress.setMainAddressLine(singleLine)
    singleLineAddress.setCountry(address.getCountry)
    singleLineAddress
  }

  private def isFirstPassMatch(candidate: Candidate) = {
    (candidate.getPrecisionCode != null &&
      MatchType.withNameWithDefault(candidate.getPrecisionCode).id == 10) ||
      getMatchScore(candidate) >= 80.0
  }

  private def getMatchScore(candidate: Candidate): Double = {
    if(candidate.getCustomFieldValue(MATCH_SCORE) != null)
      candidate.getCustomFieldValue(MATCH_SCORE).toDouble
    else 
    	0.0
  }
}