$ ->
  params =
    width: $(document).width()
    height: $(document).height()
  two = $("#two-main").append new Two(params)
  circle = two.makeCircle(72, 100, 50)
  return
