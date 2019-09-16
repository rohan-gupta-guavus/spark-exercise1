package com.assignment.spark.bo

object contentMapper {

  def getContentmap : scala.collection.mutable.Map[String,String] = scala.collection.mutable.Map[String,String]("plain" ->"text" ,
    "css" ->"text" , "javascript" ->"text" , "pdf" ->"text" , "json" ->"text" , "calender" ->"text" , "html" ->"text" ,
    "jscript" ->"text" , "livescript" ->"text" , "x-ecmascript" ->"text"  , "tiff" ->"image" , "webp" ->"image"  ,
    "gif" ->"image" , "svg+xml" ->"image" , "jpeg" ->"image" , "jpg" ->"image" , "png" ->"image" , "icon" ->"image" ,
    "tmw-max" ->"image" , "x-j2c" ->"image" , "apng" ->"image" , "bmp" ->"image" , "mp3" ->"audio" , "aac" ->"audio" ,
    "wave" ->"audio" , "wav" ->"audio" , "x-wav" ->"audio" , "x-pn-wav" ->"audio" , "webm" ->"audio" , "ogg" ->"audio" ,
    "MP3" ->"audio" , "AAC" ->"audio" , "HE-AAC" ->"audio" , "AC3" ->"audio" , "ac3" ->"audio" , "EAC3" ->"audio" ,
    "eac3" ->"audio" , "3gp" ->"video" , "3gp2"->"video" , "3g2" ->"video" , "3gpp" ->"video" , "3gpp2" ->"video" ,
    "mp4" ->"video" , "m4a" ->"video" , "m4v" ->"video" , "f4v" ->"video" , "f4a" ->"video" , "m4b" ->"video" ,
    "m4r" ->"video" , "f4b" ->"video" , "mov" ->"video" , "webm" ->"video" , "FLV" ->"video" , "flv" ->"video" ,
    "AVI" ->"video" , "avi" ->"video" , "QuickTime" ->"video" , "HDV" ->"video" , "XDCAM" ->"video" , "MPEG1" ->"video" ,
    "MPEG2" ->"video" , "mpeg" ->"video" , "multipart" ->"byteranges")

}
