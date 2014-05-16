
function get_active_tsid() {
  return $("#active_tsid").attr("tsid");
}

function render_sidebar() {

  $.get("api/tsets", function(data) {
    var template = $("#sidebar_template").html();
    var json = $.parseJSON(data);
    $("#sidebar").html(
        _.template(template, {
            items:json
          }
        )
    );
  });
}

$(function() {
  render_sidebar();
});
