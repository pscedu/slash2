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
  var data = new google.visualization.DataTable();
          data.addColumn('string', 'Name');
          data.addColumn('number', 'Salary');
          data.addColumn('boolean', 'Full Time Employee');
          data.addRows([
            ['Mike',  {v: 10000, f: '$10,000'}, true],
            ['Jim',   {v:8000,   f: '$8,000'},  false],
            ['Alice', {v: 12500, f: '$12,500'}, true],
            ['Bob',   {v: 7000,  f: '$7,000'},  true]
          ]);
  var table = new google.visualization.Table($("#test_table")[0]);
  table.draw(data, {});
}

$(function() {
  render_treemap("write");
  render_table();
});
