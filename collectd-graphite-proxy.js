"use strict"

/* TODO(sissel): make connections retry/etc
 * TODO(sissel): make graphite target configurable via command line
 *
 * This code is a work in progress.
 *
 * To use this, put the following in your collectd config:
 *
 * LoadPlugin write_http
 * <Plugin write_http>
 *   <URL "http://monitor:3012/post-collectd">
 *   </URL>
 * </Plugin>
 *
 * This will make collectd write 'PUTVAL' statements over HTTP to the above URL.
 * This code below will then convert the PUTVAL statements to graphite metrics
 * and ship them to 'monitor:2003'
 */
var http = require("http");
var net = require("net");
var assert = require("assert");
var fs = require('fs');
var dgram = require('dgram');

var types = fs.readFileSync('/usr/share/collectd/types.db', 'utf8').split("\n");

var typesObj = {};

var type_comments_re = /^#/;
var type_cut_re = /^([^\s]+)\s+(.*)/;

for (var i in types) {
  if (!type_comments_re.exec(types[i])) {
    var typeSet = type_cut_re.exec(types[i])
    if (!typeSet) { continue; }
    for (var t=0;t < typeSet.length;t++) {
      var name = typeSet[1];
      typesObj[name] = [];
      var eachType = typeSet[2].split(", ")
      for (var u=0; u < eachType.length; u++){
        var theName = eachType[u].split(":")[0];
        typesObj[name].push(theName);
      }
    }
  }
}

var graphiteHost = process.argv[2],
    api_key = process.argv[3],
    port = process.env.COLLECTD_PROXY_PORT || 3015;

console.log('Initialized for host ' + graphiteHost);

var graphite_connection = dgram.createSocket('udp4');

var request_handler = function(request, response) {
  var putval_re = /^PUTVAL ([^ ]+)(?: ([^ ]+=[^ ]+)?) ([0-9.]+)(:.*)/;
  var chunks = [];
  request.addListener("data", function(chunk) {
    chunks.push(chunk.toString());
  });
  request.addListener("end", function(chunk) {
    var body = chunks.join("");
    if (parseInt(request.headers["content-length"]) != body.length) {
      console.log("Content-Length: %d != body.length: %d\n",
        request.headers["content-length"], body.length);
    }
    var metrics = body.split("\n");
    for (var i in metrics) {
      var m = putval_re.exec(metrics[i]);
      if (!m) {
        continue;
      }
      var values = m[4].split(":");

      for (var v in values) {
        
        var name = m[1];
        var options = m[2];
        var time = m[3];

        if ( v == 0 ) {
          continue;
        }

        // Replace some chars for graphite, split into parts
        var name_parts = name.replace(/\./g, "_").replace(/\//g, ".").split(".");

        // Start to construct the new name
        var rebuild = ["agents"]

        var host = name_parts[0].split(/_/)[0]
        rebuild = rebuild.concat(host)

        // Plugin names can contain an "instance" which is set apart by a dash
        var plugin = name_parts[1].split("-")
        rebuild = rebuild.concat(plugin[0])
        if (plugin.length > 1) {
          var plugin_instance = plugin.slice(1).join("-")
          rebuild = rebuild.concat(plugin_instance)
        }
        plugin = plugin[0]

        // Type names can also contain an "instance"
        var type = name_parts[2].split("-")
        if (type[0] != plugin) {
          // If type and plugin are equal, delete one to clean up a bit
          rebuild = rebuild.concat(type[0])
        }
        if (type.length > 1) {
          var type_instance = type.slice(1).join("-")
          rebuild = rebuild.concat(type_instance)
        }
        type = type[0]

        // Put the name back together
        name = rebuild.join(".")
        
        if ( values.length > 2 ) {
          var metric = name_parts[2];
          var index;

          //  If the metric contains a '-' (after removing the instance name)
          //  then we want to remove it before looking up in the types.db
          index = metric.search(/-/)
          if (index > -1) {
            metric = /^([\w]+)-(.*)$/.exec(metric);
          } else {
            // Kinda a hack
            metric = [ "", metric]
          }
          name = name + "." + typesObj[metric[1]][v - 1];
        }

        name = api_key + '.' + name;
        var message = [name, values[v], time].join(" ");

        console.log(message);

        var m = new Buffer(message + '\n');
        graphite_connection.send(m, 0, m.length, 2003, graphiteHost);
      }

    }
  });

  request.addListener("end", function() {
    response.writeHead(200, {"Content-Type": "text/plain"});
    response.write("OK");
    response.end();
  });
};

console.log('Listening on port ' + port);

var server = http.createServer();
server.addListener("request", request_handler);
server.listen(port);
