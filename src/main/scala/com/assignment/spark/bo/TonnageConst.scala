package com.assignment.spark.bo

object TonnageConst {


  def getUplink: String = "transaction_uplink_bytes"

  def getDownLink: String = "transaction_downlink_bytes"

  def getReplyCodeCol: String = "http_reply_code"

  def getSumUplink: String = "SumUplinkBytes"

  def getSumDownlink: String = "SumDownlinkBytes"

  def getSumTotalBytes: String = "TotalBytes"

  def getHitsCol: String = "Hits"

  def getRadiusUserName : String = "radius_user_name"

  def getRankUplink : String = "rank_uplink"

  def getRankDownlink : String = "rank_downlink"

  def getRankHits : String = "rank_hits"

  def getTotal : String = "Total"

  def getSumTotal : String = "SumTotal"

  def getPercentCont : String = "Percentage_Contribution"

}
