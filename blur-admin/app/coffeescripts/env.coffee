$(document).ready ->
  width = ($('#hdfs-status').css("width").substring(0, 3) - 50) + "px"
  $('.bottom-status').css("width", width)
  
  $(window).resize ->
    width = ($('#hdfs-status').css("width").substring(0, 3) - 50) + "px"
    $('.bottom-status').css("width", width)