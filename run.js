#!/usr/bin/env node

var url = require("url");
var program = require('commander');
var Importer = require('./lib/importer');

program
    .version('0.0.1')
    .option('-p, --path', 'Enter s3 bucket and prefix in format s3://bucket/prefix')
    .parse(process.argv);

if(program.path) {
    var path = url.parse(program.args[0]);
    var importer = new Importer([]);
    importer.getS3Records(path.host, path.pathname)
        .then(function(records) {
            importer.records = records;
            importer.run();
        })
        .catch(function(error) {
            console.log(error);
        });

} else {
    console.log('Please provide an s3 path parameter.');
    process.exit(1);
}
