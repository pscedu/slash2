var test_data ={
 "zero": [
  {
   "client": "localhost",
   "result": {
    "resource_usage": {},
    "name": "zero",
    "operate": {
     "elapsed": 0.8008420467376709,
     "error": null,
     "pass": true
    }
   }
  },
  {
   "client": "127.0.0.1",
   "result": {
    "resource_usage": {},
    "name": "zero",
    "operate": {
     "elapsed": 0.8008420467376709,
     "error": null,
     "pass": true
    }
   }
  }
 ],
 "read": [
  {
   "client": "localhost",
   "result": {
    "resource_usage": {},
    "name": "read",
    "operate": {
     "elapsed": 2.0020549297332764,
     "error": null,
     "pass": true
    }
   }
  },
  {
   "client": "127.0.0.1",
   "result": {
    "resource_usage": {},
    "name": "read",
    "operate": {
     "elapsed": 2.0020549297332764,
     "error": null,
     "pass": true
    }
   }
  }
 ],
 "buffer": [
  {
   "client": "localhost",
   "result": {
    "resource_usage": {},
    "name": "buffer",
    "operate": {
     "elapsed": 1.2012419700622559,
     "error": null,
     "pass": true
    }
   }
  },
  {
   "client": "127.0.0.1",
   "result": {
    "resource_usage": {},
    "name": "buffer",
    "operate": {
     "elapsed": 1.2012419700622559,
     "error": null,
     "pass": true
    }
   }
  }
 ],
 "write": [
  {
   "client": "localhost",
   "result": {
    "resource_usage": {},
    "name": "write",
    "operate": {
     "elapsed": 1.501253890991211,
     "error": null,
     "pass": true
    }
   }
  },
  {
   "client": "127.0.0.1",
   "result": {
    "resource_usage": {},
    "name": "write",
    "operate": {
     "elapsed": 0.901253890991211,
     "error": null,
     "pass": false
    }
   }
  }
 ]
}
 
google.load("visualization", "1", {packages:["treemap", "table"]});

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

  var data = [
    ['Test', 'Parent', 'Time (size)', 'Time (color)'],
    ["Clients", null, 0, 0]
  ];
  
  _.each(test_data[active_test], function(test) {
    var size = test.result.operate.elapsed;
    var color = test.result.operate.pass ? 0 : 100;
    data.push([test.client, "Clients", size, color]);
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
}

function render_table() {
  var items = [];
  _.map(test_data, function(results, test) {
    var pass_fails = get_pass_fail(results);
    items.push({
      test: test,
      passed: pass_fails[0],
      failed: pass_fails[1],
      time: get_average_elapsed(results),
      change: 0
    });
  });

  var template = $("#table_template").html();
  $("#test_table").html(_.template(template,{items:items}));
  
  $(".tree_link").click(function (){
    render_treemap($(this).text());
  });
}

$(function() {
  var first_test = _.first(_.keys(test_data));
  render_treemap(first_test);
  render_table();
});
