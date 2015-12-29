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
var pgCopy = require('pg-copy-streams').from;
var pg = require('pg');
var parse = require('csv-parse');
var async = require('async');
var Q = require('q');
var pgp = require('pg-promise')({ promiseLib: Q });
var db;

var getS3 = Q.nbind(s3.getObject, s3);

exports.handler = function(event, context) {
    
    exports.getColumnType = function(value) {
        var type = "text";
        //if(value.match(/^(\d+\.?\d+)$/)) type = "numeric";
        return type;
    };

    exports.getTableName = function(key, inputInfo) {
        var table = key.split('/');
        if(inputInfo.folderDepthLevelForTableName < 0) {
            table.reverse();
        folderDepthLevelForTableName = (inputInfo.folderDepthLevelForTableName * -1) - 1;
        }

        return inputInfo.schema + '.' + inputInfo.tablePrefix + table[folderDepthLevelForTableName].replace(/[\W]+/g,'_');  
    };

    exports.getTableNames = function(inputInfo) {
        return new Q.promise(function(resolve, reject) {
            for(var i = 0; i < event.Records.length; i++) {
                var key = event.Records[i].s3.object.key;
                var tableName = exports.getTableName(key, inputInfo);
                if(inputInfo.tableNames.indexOf(tableName) === -1) {
                    inputInfo.tableNames.push({
                        'tableName': tableName,
                        'key': key
                    });
                    console.log("Resolving table name to: " + tableName);
                }
            }
            resolve(inputInfo);
        });
    };

    exports.createTablesIfNotExists = function(inputInfo) {
        var createTables = [];
        var table = inputInfo.tableNames[0];
        var deferred = Q.defer();

        var createTableIfNotExists = function(table) {    
            var createDeferred = Q.defer();
            getS3({Bucket: inputInfo.bucket, Key: table.key})
                .then(function(response) {
                    parse(response.Body.toString(), {delimiter: inputInfo.delimiter}, function(error, output) {
                        var sql = "CREATE TABLE IF NOT EXISTS " + table.tableName + "(";
                        var columns = [];
                        for(var i = 0; i < output[0].length; i++) {
                            columns.push(output[0][i] + " " + exports.getColumnType(output[1][i]));
                        }
                        sql += columns.join(',') + ')';
                        console.log(sql);
                          
                        db.query(sql)
                            .then(function() {
                                createDeferred.resolve();
                            })
                            .catch(function(error) {
                                createDeferred.reject(error);
                            });
                    });
                });
                return createDeferred.promise;
        };

        for(var i = 0; i < inputInfo.tableNames.length; i++) {
            createTables.push(createTableIfNotExists(inputInfo.tableNames[i]));
        }

        Q.allSettled(createTables).then(function(response) {
            deferred.resolve(inputInfo);
        })

        return deferred.promise;

    };

    exports.updateConfig = function(inputInfo) {
        console.log("Found RDS Configuration for " + inputInfo.bucket);

        var config = inputInfo.config.Item;
        var decryptMap = Q.nbind(kmsCrypto.decryptMap, kmsCrypto);
        var deferred = Q.defer();
        var encryptedItems = {
            'rdsPassword': kmsCrypto.stringToBuffer(config.loadRDS.L[0].M.connectPassword.S)
        };

        return decryptMap(encryptedItems)
            .then(function(response) {
                inputInfo.tablePrefix = config.loadRDS.L[0].M.tablePrefix.S;
                inputInfo.schema = config.loadRDS.L[0].M.targetSchema.S;
                inputInfo.folderDepthLevelForTableName = config.folderDepthLevelForTableName.N;
                inputInfo.truncateTarget = config.loadRDS.L[0].M.truncateTarget.BOOL;
                inputInfo.connectionString = 'postgres://' + config.loadRDS.L[0].M.connectUser.S + ':' + response.rdsPassword.toString() + '@' + config.loadRDS.L[0].M.rdsHost.S + ':' + config.loadRDS.L[0].M.rdsPort.N + '/' + config.loadRDS.L[0].M.rdsDB.S;
                
                db = pgp(inputInfo.connectionString);
                return inputInfo;
            })
    };

    exports.dropTablesIfExists = function(inputInfo) {
        var dropTables = [];
        var deferred = Q.defer();

        var dropTableIfExists = function(table) {
            var sql = "DROP TABLE IF EXISTS " + table;
            console.log(sql);
            return db.query(sql);
        };

        if(inputInfo.truncateTarget) {
            for(var i = 0; i < inputInfo.tableNames.length; i++) {
                dropTables.push(dropTableIfExists(inputInfo.tableNames[i].tableName));
            }
        }

        Q.allSettled(dropTables).then(function() {
            deferred.resolve(inputInfo);
        });

        return deferred.promise;
    };

    exports.runImport = function(inputInfo) {        
        console.log('Importing data.');
        var deferred = Q.defer();

        function fetchRecord(i) {
            var key = event.Records[i].s3.object.key;
            var stream = S3S.ReadStream(s3, {Bucket: inputInfo.bucket, Key: key});
            pg.connect(inputInfo.connectionString, function(error, client) {
                if(error) {
                    reject(error);
                } else {
                    var tableName = exports.getTableName(key, inputInfo);
                    var query = client.query(pgCopy(
                           "COPY " + tableName + " FROM STDIN WITH CSV HEADER DELIMITER '" + inputInfo.delimiter + "'"
                        ));
                    stream.pipe(query)
                        .on('end', function () {
                            console.log('Loaded file: ' + key);
                            client.end();
                            i++;
                                
                            if(i < event.Records.length) {
                                fetchRecord(i);
                            } else {
                                deferred.resolve(inputInfo);
                            }
                        })
                        .on('error', function(error) {
                            console.log(error);
                            deferred.reject(error);
                        });
                }
            });
        }

        fetchRecord(0);
        return deferred.promise;
    };

    exports.getConfig = function() {
        return new Q.promise(function(resolve, reject) {
            var getDynamoItem = Q.nbind(dynamoDB.getItem, dynamoDB);
            var inputInfo = {
                bucket: event.Records[0].s3.bucket.name,
                key: event.Records[0].s3.object.key,
                schema: 'public',
                folderDepthLevelForTableName: 0,
                tableNames: [],
                connectionString: null,
                delimiter: ',',
                truncateTarget: false,
                tablePrefix: ''
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

            return getDynamoItem(dynamoLookup)
                .then(function(response) {
                    inputInfo.config = response;
                    resolve(inputInfo);
                })
                .catch(function(error) {
                    reject(error);
                });
        });
    };

    if(!event.Records) {
        console.log('No records found');
        context.done();
    } else {
        var inputInfo = {}
        exports.getConfig()
            .then(exports.updateConfig)
            .then(exports.getTableNames)
            .then(exports.dropTablesIfExists)
            .then(exports.createTablesIfNotExists)
            .then(exports.runImport)
            .catch(function(error) {
                console.log(error);
                context.done(error);
            })
            .finally(function() {
                pgp.end();
                context.done();
            });
    }
};