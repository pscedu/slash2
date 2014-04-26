google.load("visualization", "1", {packages:["corechart"]});

$(function() {

  var blue = "#0F4D92";
  var green = "#00703C";
  var red = "#A45A52";

  var change_thresh = 10.0;

  var test_names = [];
  var test_change_data = [];
  var change_colors = [];


  $("a.test-name").each(function() {
    test_names.push(this.text);
  });

  $(".change-delta").each(function() {
    var val = parseFloat($(this).text());
    test_change_data.push(val);
    if(val >= change_thresh) {
      change_colors.push(red);
    } else if(val <= -change_thresh) {
      change_colors.push(green);
    } else {
      change_colors.push(blue);
    }
  });

  var change_gdata = [];

  change_gdata.push(["Test", "Change", {role:"style"}, {role:"annotation"}]);

  $.each(test_names, function(i, v) {
    if(test_change_data[i] !== undefined)
      change_gdata.push([v, test_change_data[i], change_colors[i], test_change_data[i] + "%" ]);
  });


  console.log(change_colors);
  var data = google.visualization.arrayToDataTable(change_gdata);

  var options = {
    title: 'Recent Test Performance',
    height: 350,
    legend: {
      position: "none"
    },
    vAxis: {
      minValue: -10,
      maxValue: 10
    }
  };

  var chart = new google.visualization.ColumnChart(document.getElementById('chart_div'));
  chart.draw(data, options);
});
