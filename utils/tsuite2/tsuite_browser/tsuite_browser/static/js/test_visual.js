google.load("visualization", "1", {packages:["corechart"]});
$(function() {
  $(".test").each(function() {
    var test_name = $(this).attr("name");
    var test_data = [["tsid", "time"]];
    $(this).children(".test-data").each(function(i, e) {
      test_data.push([$(e).attr("tsid"), parseFloat($(e).attr("data"))]);
    });

    var data = google.visualization.arrayToDataTable(test_data);
    var options = {
      title: null,
      legend: {
        "position": "none"
      }
    };
    var chart = new google.visualization.LineChart($("#"+test_name)[0]);
    chart.draw(data, options); 
  });

});
