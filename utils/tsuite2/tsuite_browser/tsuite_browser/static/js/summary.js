google.load("visualization", "1", {packages:["treemap"]});

function render_change_table() {
  var active_call = "api/tsets/display/"+get_active_tsid();
  $.get(active_call, function(data) {
    var template = $("#change_table_template").html()
    var json = $.parseJSON(data)
    var compiled = _.template(template, {
      items:json
    });
    $("#change_table").html(compiled);
    
    $(".tree_link").click(function (){
      render_treemap($(this).text());
    });

    var first_test = $(".tree_link")[0].text;
    render_treemap(first_test);
  });
}

function get_average_elapsed(results) {
  return _.reduce(results, function(memo, next) {
    return memo + next.result.operate.elapsed;
  }, 0) / results.length;
}

function get_pass_fail(results) {
  var pass = _.reduce(results, function(memo, next) {
    var passInt = next.result.operate.pass ? 1 : 0;
    return memo + passInt;
  },0);
  return [pass, results.length];
}

function render_treemap(active_test) {

  var treemap_call = "api/tsets/"+get_active_tsid();
  $.get(treemap_call, function(api_data) {
  
    api_data = $.parseJSON(api_data);

    var data = [
      ['Test', 'Parent', 'Time (size)', 'Time (color)'],
      ["Clients", null, 0, 0]
    ];
  
    var test = _.find(api_data["tests"], function(test) {
      return test.hasOwnProperty(active_test);
    });

    _.each(test[active_test], function(client) {
      console.log(client);
      var size = client.result.elapsed;
      var color = client.result.pass ? 0 : 100;
      data.push([client.client, "Clients", size, color]);
    });
    

    // Create and draw the visualization.
    var tree = new google.visualization.TreeMap($("#active_treemap")[0]);
    tree.draw(google.visualization.arrayToDataTable(data), {
      minColor: '#D3D3D3',
      midColor: '#D3D3D3',
      maxColor: '#FF0000',
      headerHeight: 15,
      fontColor: 'black',
      title: active_test + " Results",
      showScale: false,
      useWeightedAverageForAggregation: true
    });
  });

}

$(function() {
  render_change_table();
  console.log($(".tree_link")); 

});
