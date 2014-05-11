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
     "elapsed": 1.201253890991211,
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
     "elapsed": 1.201253890991211,
     "error": null,
     "pass": true
    }
   }
  }
 ]
}
 
google.load("visualization", "1", {packages:["treemap"]});

function get_test_size(results) {
  var sum = _.reduce(results, function(memo, next) {
    return memo + next.result.operate.elapsed;
  }, 0);

  return sum;
}

function get_test_color(results) {
  var sum = _.reduce(results, function(memo, next) {
    return memo + next.result.operate.elapsed;
  }, 0);

  return sum;
}

function get_average_elapsed(results) {
  return _.reduce(results, function(memo, next) {
    return memo + next.result.operate.elapsed;
  }, 0) / results.length;
}

$(function() {

  var data = [
    ['Test', 'Parent', 'Time (size)', 'Time (color)'],
    ['Tests', null, 0, 0]
  ];
  
  _.map(test_data, function(results, test_name) {
    var size = get_test_size(results);
    var color = get_test_color(results);
    
    data.push([test_name, "Tests", size, color]);

    _.each(results, function(test) {
      console.log(test);
      data.push([test_name+"@"+test.client, test_name, test.result.operate.elapsed, test.result.operate.elapsed]);
    });
  });
  
  // Create and draw the visualization.
  var tree = new google.visualization.TreeMap($("#client_treemap")[0]);
  tree.draw(google.visualization.arrayToDataTable(data), {
    minColor: '#f00',
    midColor: '#ddd',
    maxColor: '#0d0',
    headerHeight: 15,
    fontColor: 'black',
    title: "Test Results",
    showScale: false,
    useWeightedAverageForAggregation: true
  });


});
