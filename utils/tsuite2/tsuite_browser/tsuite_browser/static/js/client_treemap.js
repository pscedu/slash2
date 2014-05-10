var test_data = [('localhost', {'tests': [{'setup': {'pass': True, 'error': None}, 'operate': {'pass': True, 'elapsed': 1.5020370483398438e-05, 'error': None}, 'name': 'sl2_test1', 'resource_usage': {}, 'cleanup': {'pass': True, 'error': None}}, {'setup': {'pass': True, 'error': None}, 'operate': {'pass': True, 'elapsed': 1.2002780437469482, 'error': None}, 'name': 'sl2_test2', 'resource_usage': {}, 'cleanup': {'pass': True, 'error': None}}]}), ('127.0.0.1', {'tests': [{'setup': {'pass': True, 'error': None}, 'operate': {'pass': True, 'elapsed': 1.5020370483398438e-05, 'error': None}, 'name': 'sl2_test1', 'resource_usage': {}, 'cleanup': {'pass': True, 'error': None}}, {'setup': {'pass': True, 'error': None}, 'operate': {'pass': True, 'elapsed': 1.2002780437469482, 'error': None}, 'name': 'sl2_test2', 'resource_usage': {}, 'cleanup': {'pass': True, 'error': None}}]})]


google.load("visualization", "1", {packages:["treemap"]});
      google.setOnLoadCallback(drawChart);
      function drawChart() {
        // Create and populate the data table.
        var data = google.visualization.arrayToDataTable([
          ['Test', 'Parent', 'Market trade volume (size)', 'Market increase/decrease (color)'],
          ['Ran Tests',    null,                 0,                               0],
          ['Zaire',     'Africa',             8,                               10]
        ]);

        // Create and draw the visualization.
        var tree = new google.visualization.TreeMap(document.getElementById('chart_div'));
        tree.draw(data, {
          minColor: '#f00',
          midColor: '#ddd',
          maxColor: '#0d0',
          headerHeight: 15,
          fontColor: 'black',
          showScale: true});
        }
