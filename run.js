#!/usr/bin/env node

var url = require("url");
var program = require('commander');
var Importer = require('./lib/importer');

program
    .version('0.0.1')
    .arguments('<path> [table]')
    .option('-d, --debug', 'Debug to console')
    .option('<path>', 'Enter s3 bucket and prefix in format s3://bucket/prefix')
    .option('[table]', 'Override table name in single table mode')
    .action(function (path, table) {
        program.pathValue = path;
        program.tableValue = table;
    })
    .parse(process.argv);

if (!program.args.length) {
    program.help();
    process.exit(1);
} else if(typeof program.pathValue === 'undefined') {
    console.log('Enter s3 bucket and prefix in format s3://bucket/prefix');
    process.exit(1);
} else {
    var path = url.parse(program.pathValue);
    var importer = new Importer([], program.debug);
    importer.getS3Records(path.host, path.pathname)
        .then(function(records) {
            importer.records = records;
            if(typeof program.tableValue !== 'undefined') {
                importer.defaultTableName = program.tableValue;
            }
            importer.run().then(function(response) {
                console.log('Done.');
                process.exit(response);
            })
        })
        .catch(function(error) {
            console.log(error);
        });
}