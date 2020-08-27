package com.precisely.bigdata.spectrum.global.spark.geocodingUserDefineFunction

import com.mapinfo.geocode.GeocoderPreferences
import com.mapinfo.geocode.api.DistanceUnit

class MultipassReverseGeocodeExecutor(searchDistance: Int) extends com.pb.bigdata.geocoding.api.ReverseGeocodeExecutor {
  import com.mapinfo.geocode.api.{GeocodingAPI, Preferences, Response}

  def isAcceptableMatch(response: Response): Boolean = {
    return response.getCandidates.size() > 0 && response.getCandidates.get(0).getCustomFieldValue("PB_KEY") != null
  }

  @throws(classOf[com.mapinfo.geocode.GeocodingException])
  @Override
  override def call(longitude: Double, latitude: Double, coordinatedSystem: String, country: String, preferences: Preferences, geocoder: GeocodingAPI) : Response  = {
    var response: Response = null
    var geocodePass: String = "default"
    response = geocoder.reverseGeocode(longitude,latitude,coordinatedSystem,country,preferences)
    if(!isAcceptableMatch(response)) {
      val searchDistancePreferences = new GeocoderPreferences(preferences)
      searchDistancePreferences.addCustomPreference("FIND_APPROXIMATE_PBKEY","true")
      searchDistancePreferences.setDistanceUnits(DistanceUnit.FEET)
      searchDistancePreferences.setDistance(searchDistance)
      geocodePass = "custom search distance"
      response = geocoder.reverseGeocode(longitude,latitude,coordinatedSystem,country,searchDistancePreferences)
    }
    response.getCandidates.get(0).getAddress.addCustomField("PASS_TYPE",geocodePass)
    return response
  }
}
