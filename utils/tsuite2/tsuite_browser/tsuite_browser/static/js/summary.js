google.load("visualization", "1", {packages:["treemap"]});

function exists(p) {
  return typeof(p) !== 'undefined';
}

function render_change_table() {
  var active_call = "/api/tsets/display/"+get_active_tsid();
  $.get(active_call, function(data) {
    var template = $("#change_table_template").html()
    var json = $.parseJSON(data)
    var compiled = _.template(template, {
      items:json
    });
    $("#change_table").html(compiled);
    
   /*$(".tree_link").click(function (){
      render_treemap($(this).text());
    });

    var first_test = $(".tree_link")[0].text;
    render_treemap(first_test);*/
  });
}

function render_client_blob() {
  var active_call = "/api/tsets/"+get_active_tsid();

  $.get(active_call, function(data) {
    var template = $("#client_blob_template").html()
    var json = $.parseJSON(data)

    client_objs = {};
    _.each(json.tests, function(test) {
      var clients = _.flatten(_.values(test));
      _.each(clients, function(client) {
        if(!client_objs.hasOwnProperty(client.client)) {
          client_objs[client.client] = {
            name: client.client,
            tests: []
          };
        }
        client_objs[client.client].tests.push(client.result);
      });
    });
    
    var compiled = _.template(template, {
      items:client_objs
    });
    $("#client_blob").html(compiled);
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


$(function() {
  render_change_table();
  render_client_blob();
});
