$(function() {
  function shadeColor2(color, percent) {   
      var f=parseInt(color.slice(1),16),t=percent<0?0:255,p=percent<0?percent*-1:percent,R=f>>16,G=f>>8&0x00FF,B=f&0x0000FF;
      return "#"+(0x1000000+(Math.round((t-R)*p)+R)*0x10000+(Math.round((t-G)*p)+G)*0x100+(Math.round((t-B)*p)+B)).toString(16).slice(1);
  }
  $.get("/data", function( data ) {
    data = eval("("+data+")");
    var size = 800;
    var c = d3.select("#main").append("svg").attr("width", size).attr("height", size);
    var g = c.append('g').attr("transform" ,"scale(0)");
    
    var margin = 10;
    var pad = 5;
    var x = margin;
    var y = margin;

    var innerMargin = 5;
    var innerXPad = 2;
    var innerYPad = 6;


    var text_pad = 14;

    var width = (size - 2*margin - 3*pad)/4;
    var height = (size - 2*margin - pad)/2;

    var diskWidth = (width - 2 * innerMargin - 2 * innerXPad) / 3;
    var diskHeight = (height - 2 * innerMargin - 3 * innerYPad) / 4 - text_pad * 1.5;
    
    for(i = 0; i < 8; i++) {
      g.append("rect").attr("x", x).attr("y", y).attr("width", width).attr("height", height).attr("fill", "black");
      g.append("text").attr("x", x + width/2 - 28).attr("y", y + text_pad).text(function (d) {
        return("sense " + i);
      }).attr("font-family", "sans-serif").attr("font-size", "12px").attr("fill", "white").attr("text-anchor", "middle");
      g.append("line").attr("x1", x).attr("y1", y+text_pad+6).attr("x2", x + width).attr("y2", y+text_pad+6).attr("stroke", "#fff");
      

      var ix = x + innerMargin;
      var iy = y + innerMargin + text_pad * 2;

      j = 0;

      max = 1;
      $.each(data["sense"+i], function(k, v) {
        if(v > max) {
          max = v;
        }
      });


      $.each(data["sense"+i], function(k, v) {
        color = 1 - v/max;
        g2 = g.append('g').attr("transform" ,"scale(0)");
        g2.append("rect").attr("x", ix).attr("y", iy).attr("width", diskWidth).attr("height", diskHeight).attr("fill", shadeColor2("#FF0000", color));
        g2.append("text").attr("x", ix + diskWidth/2 - 25).attr("y", iy+text_pad).text(function (d) {
          return (k);
        }).attr("font-family", "sans-serif").attr("font-size", "8px").attr("fill", "black");

        g2.append("text").attr("x", ix + diskWidth/2 - 10).attr("y", iy+text_pad*4).attr("font-size", "8px").text(function (d) {
          return v;
        });

        ix += diskWidth + innerXPad;
        if((j+1) % 3 == 0) {
          iy += diskHeight + innerYPad;
          ix = x + innerMargin;
        }
        j++;
        g2.transition().duration(500).attr("transform" ,"scale(1)");
      });
      
      x += width + pad;
      if(i == 3) {
        x = margin;
        y += height + pad;
      }
    }

    g.transition().duration(500).attr("transform" ,"scale(1)");

  });
});
