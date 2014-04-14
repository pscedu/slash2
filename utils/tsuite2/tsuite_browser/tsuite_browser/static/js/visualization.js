google.load("visualization", "1", {packages:["corechart"]});

$(function() {
  var change_thresh = 5.0;

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
      change_colors.push("#b20000");
    } else if(val <= -change_thresh) {
      change_colors.push("#009900");
    } else {
      change_colors.push("#0000b2");
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
    }
  };

  var chart = new google.visualization.ColumnChart(document.getElementById('chart_div'));
  chart.draw(data, options);
});
