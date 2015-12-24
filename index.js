var region = process.env['AWS_REGION'];
if (!region || region === null || region === "") {
    region = "us-east-1";
    console.log("AWS Lambda RDS Database Loader using default region " + region);
}

var aws = require('aws-sdk');
aws.config.update({
    region : region
});
var s3 = new aws.S3({
    apiVersion : '2006-03-01',
    region : region
});
var dynamoDB = new aws.DynamoDB({
    apiVersion : '2012-08-10',
    region : region
});

require('./lib/constants');
var common = require('./lib/common');
var kmsCrypto = require('./lib/kmsCrypto');
kmsCrypto.setRegion(region);
var S3S = require('s3-streams');
var pg = require('pg');
var pgCopy = require('pg-copy-streams').from;
var parse = require('csv-parse');
var async = require('async');

exports.handler = function(event, context) {
    
    exports.getColumnType = function(value) {
        var type = "text";
        if(new Date(value) !== "Invalid Date" && !isNaN(new Date(value))) type = "datetime";
        if(value.match(/^(\d+\.?\d{0,9}|\.\d{1,9})$/)) type = "numeric";
        return type;
    };

    exports.getTableName = function(inputInfo) {
        var table = inputInfo.key.split('/');
        if(inputInfo.folderDepthLevelForTableName < 0) {
            table.reverse();
            inputInfo.folderDepthLevelForTableName = (inputInfo.folderDepthLevelForTableName * -1);
        }
        table = table[inputInfo.folderDepthLevelForTableName].replace(/[\W]+/g,'_');
        return table;
    };

    exports.createTableIfNotExists = function(inputInfo, callback) {
        if(inputInfo.tableName === null) {
            var msg = 'Table name cannot be null.';
            console.log(msg);
            context.done(null, msg);
        } else {
            s3.getObject({Bucket: inputInfo.bucket, Key: inputInfo.key}, function(err, response) {
                if(err) {
                    console.log(err);
                    context.done(err, null);
                }
                parse(response.Body.toString(), {delimiter: inputInfo.delimiter}, function(err, output) {
                    var sql = "CREATE TABLE IF NOT EXISTS " + inputInfo.tableName + "(";
                    var columns = [];
                    for(var i = 0; i < output[0].length; i++) {


                        columns.push(output[0][i] + " " + exports.getColumnType(output[1][i]));
                    }

                    if(columns.length === 0) {
                        context.done(null, 'Column count for new table cannot be 0.');
                    } else {
                        pg.connect(inputInfo.connectionString, function(err, client) {
                            if(err) {
                                context.done(err, null);
                            }
                            sql += columns.join(',') + ')';
                            console.log(sql)
                            client.query(sql);
                            callback();
                        });
                    }
                });
            });
        }
    };

    exports.foundConfig = function(inputInfo, err, data) {
        if (err) {
            console.log(err);
            var msg = 'Error getting RDS Configuration for ' + inputInfo.bucket + ' from Dynamo DB ';
            console.log(msg);
            context.done(error, msg);
        } else {
            if (!data || !data.Item) {
                console.log("No Configuration Found for " + inputInfo.bucket);
                context.done(null, null);
            } else {
                console.log("Found Redshift RDS Configuration for " + inputInfo.bucket);
                var config = data.Item;

                var encryptedItems = {};
                encryptedItems['rdsPassword'] = kmsCrypto.stringToBuffer(config.loadRDS.L[0].M.connectPassword.S);
                kmsCrypto.decryptMap(encryptedItems, function(err, decryptedConfigItems) {
                    if (err) {
                        context.done(error, 'Error decrypting configuration.');
                    } else {
                        inputInfo.connectionString = 'postgres://' + config.loadRDS.L[0].M.connectUser.S + ':' + decryptedConfigItems.rdsPassword.toString() + '@' 
                            + config.loadRDS.L[0].M.rdsHost.S + ':' + config.loadRDS.L[0].M.rdsPort.N + '/' + config.loadRDS.L[0].M.rdsDB.S;
                        inputInfo.schema = config.loadRDS.L[0].M.targetSchema.S;
                        inputInfo.folderDepthLevelForTableName = config.folderDepthLevelForTableName.N;
                        exports.runImport(inputInfo);
                    }
                });
            }
        }
    };

    exports.runImport = function(inputInfo) {
        for(var i = 0; i < event.Records.length; i++) {
            if(i === 0) {
                inputInfo.key = event.Records[i].s3.object.key;
                var name = exports.getTableName(inputInfo);
                inputInfo.tableName = (inputInfo.schema.length > 0 ? inputInfo.schema + '.' + name : name);
                console.log('Table to import data into: ' + inputInfo.tableName);
                
                exports.createTableIfNotExists(inputInfo, function() {
                    var stream = S3S.ReadStream(s3, {Bucket: inputInfo.bucket, Key: inputInfo.key});
                    pg.connect(inputInfo.connectionString, function(err, client) {
                        if (err) console.log(err);
                        var query = client.query(pgCopy(
                            "COPY " + inputInfo.tableName + " FROM STDIN WITH CSV HEADER DELIMITER '" + inputInfo.delimiter + "'"
                        ));
                        stream.pipe(query)
                            .on('end', function () {
                                client.end();
                                context.done();
                            })
                            .on('error', function(error) {
                                console.log(error);
                            });
                    });
                });
            }
        }
    };

    if(!event.Records) {
        console.log('No records found');
        context.done(null, null);
    } else {
        var inputInfo = {
            bucket: event.Records[0].s3.bucket.name,
            schema: 'public',
            folderDepthLevelForTableName: 0,
            tableName: null,
            connectionString: null,
            delimiter: ','
        };

        // load the configuration for this prefix
        var dynamoLookup = {
            Key : {
                s3Prefix : {
                    S : inputInfo.bucket
                }
            },
            TableName : configTable,
            ConsistentRead : true
        };

        var proceed = false;
        var lookupConfigTries = 10;
        var tryNumber = 0;
        var configData = null;

        async.whilst(function() {
            // return OK if the proceed flag has been set, or if we've hit the retry count
            return !proceed && tryNumber < lookupConfigTries;
        }, function(callback) {
            tryNumber++;

            // lookup the configuration item, and run foundConfig on completion
            dynamoDB.getItem(dynamoLookup, function(err, data) {
                if (err) {
                    if (err.code === provisionedThroughputExceeded) {
                        // sleep for bounded jitter time up to 1 second and then retry
                        var timeout = common.randomInt(0, 1000);
                        console.log(provisionedThroughputExceeded + " while accessing " + configTable + ". Retrying in " + timeout + " ms");
                        setTimeout(callback, timeout);
                    } else {
                        // some other error - call the error
                        callback(err);
                    }
                } else {
                    configData = data;
                    proceed = true;
                    callback(null);
                }
            });
        }, function(err) {
            if (err) {
                // fail the context as we haven't been able to lookup the onfiguration
                console.log(err);
                context.done(error, err);
            } else {
                // call the foundConfig method with the data item
                exports.foundConfig(inputInfo, null, configData);
            }
        });
    }
};