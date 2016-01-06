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

require('./constants');
var common = require('./common');
var kmsCrypto = require('./kmsCrypto');
kmsCrypto.setRegion(region);
var S3S = require('s3-streams');
var pgCopy = require('pg-copy-streams').from;
var pg = require('pg');
var parse = require('csv-parse');
var Q = require('q');
var pgp = require('pg-promise')({ promiseLib: Q });
var db;
var _ = require('underscore');
var getS3 = Q.nbind(s3.getObject, s3);
var s3Keys = require('./s3-keys');


function Importer(records) {
	var _this = this;
	_this.records = records;
};

Importer.prototype.getS3Records = function(bucket, prefix) {
	var _this = this;
    var deferred = Q.defer();
	var records = [];
	var prefix = prefix.replace(/^\/|\/$/g, '') + '/';

	var s3ListKeys = new s3Keys(s3, 1000);
		s3ListKeys.listKeys({
  			bucket: bucket,
  			prefix: prefix
		}, function (error, keys) {
  			if (error) {
    			deferred.reject(error);
  			}

  			_.each(keys, function (key) {
  				if(_.last(key).replace(/[\W_]+/g,'').length !== 0) {
  					records.push({
			            s3: {
			                bucket: {
			                    name: bucket,
			                },
			                object: {
			                    key: key
			                }
			            }
        			});
    			}
  			});
  			deferred.resolve(records);
	});

	return deferred.promise;
},

Importer.prototype.getTableName = function(key, inputInfo) {
    var table = key.split('/');
    if(inputInfo.folderDepthLevelForTableName < 0) {
        table.reverse();
    folderDepthLevelForTableName = (inputInfo.folderDepthLevelForTableName * -1) - 1;
    }
    return inputInfo.schema + '.' + inputInfo.tablePrefix + table[folderDepthLevelForTableName].replace(/[\W]+/g,'_');  
};

Importer.prototype.getTableNames = function(inputInfo) {
    return new Q.promise(function(resolve, reject) {
        if(!inputInfo.useSingleTable) {
            for(var i = 0; i < inputInfo.records.length; i++) {
                var key = inputInfo.records[i].s3.object.key;
                var tableName = Importer.prototype.getTableName(key, inputInfo);
                if(inputInfo.tableNames.indexOf(tableName) === -1) {
                    inputInfo.tableNames.push({
                        'tableName': tableName,
                        'key': key
                    });
                    console.log("Resolving table name to: " + tableName);
                }
            }                
        }

        resolve(inputInfo);
    });
};

Importer.prototype.createTablesIfNotExists = function(inputInfo) {
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
                            columns.push(output[0][i] + " text");
                        }
                        sql += columns.join(',') + ') WITH (OIDS=FALSE);';
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

Importer.prototype.updateConfig = function(inputInfo) {
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
                inputInfo.useSingleTable = config.loadRDS.L[0].M.useSingleTable.BOOL;
                inputInfo.connectionString = 'postgres://' + config.loadRDS.L[0].M.connectUser.S + ':' + response.rdsPassword.toString() + '@' 
                    + config.loadRDS.L[0].M.rdsHost.S + ':' + config.loadRDS.L[0].M.rdsPort.N + '/' + config.loadRDS.L[0].M.rdsDB.S;
                
                db = pgp(inputInfo.connectionString);
                return inputInfo;
            })
    };

Importer.prototype.dropTablesIfExists = function(inputInfo) {
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

Importer.prototype.runImport = function(inputInfo) {        
    console.log('Importing data.');
    var deferred = Q.defer();
    var _this = this;

    function importRecords(i, key, columns, tableName) {
    	return new Q.promise(function(resolve, reject) {
            var stream = S3S.ReadStream(s3, {Bucket: inputInfo.bucket, Key: key});
            pg.connect(inputInfo.connectionString, function(error, client) {
                if(error) {
                    reject(error);
                } else {
                    console.log('Loading file:', i+1, 'of', inputInfo.records.length, ' - ', key);
                    var copySql = "COPY " + tableName + " (" + columns.join(',') + ") FROM STDIN WITH CSV HEADER DELIMITER '" + inputInfo.delimiter + "'";

                    var query = client.query(pgCopy(copySql));
	                stream.pipe(query)
	                    .on('end', function () {
                            client.end();
                            i++;
	                                    
                            if(i < inputInfo.records.length) {
                                fetchRecord(i);
                            } else {
                                resolve(inputInfo);
                            }
                        })
	                    .on('error', function(error) {
	                        console.log(error);
                            console.log(copySql);
	                        reject(error);
	                    });
                }
            });
        }); 
    }

    function fetchRecord(i) {
    	var key = inputInfo.records[i].s3.object.key;
        var tableName = Importer.prototype.getTableName(key, inputInfo);
        var columns = [];

        getS3({Bucket: inputInfo.bucket, Key: key})
            .then(function(response) {
                parse(response.Body.toString(), { delimiter: inputInfo.delimiter, columns: true }, function(error, output) {
                    if(error) {
                        deferred.reject(error);
                    } else {
                        columns = Object.keys(output[0]);

                        var _this = Importer.prototype;
                        var tableName = _this.getTableName(key, inputInfo);
                        if(i === 0 && inputInfo.useSingleTable) {
                            console.log("Resolving table name to: " + tableName);
                            
                            inputInfo.tableNames.push({
                                'tableName': tableName,
                                'key': key
                            });

                            _this.dropTablesIfExists(inputInfo)
                                .then(_this.createTablesIfNotExists)
                                .then(function(response) {
                                    return importRecords(i, key, columns, tableName);
                                })
                                .catch(function(error) {
                                    deferred.reject(error);
                                });

                        } else if(inputInfo.useSingleTable) {
                            tableName = inputInfo.tableNames[0].tableName;
                            var newColumns = [];
                            for(var j = 0; j < columns.length; j++) {
                                var addColumnSql = "ALTER TABLE " + tableName + " ADD COLUMN " + columns[j] + " TEXT NULL";
                                newColumns.push(db.query(addColumnSql));
                            }

                            Q.allSettled(newColumns)
                                .then(function() { 
                                    return importRecords(i, key, columns, tableName);
                                });
                                    

                        } else {
                            return importRecords(i, key, columns, tableName);
                        }
                    }    
                });
            });
    }

    fetchRecord(0);
    return deferred.promise;
};

Importer.prototype.getConfig = function() {
    var _this = this;
    return new Q.promise(function(resolve, reject) {
    	var getDynamoItem = Q.nbind(dynamoDB.getItem, dynamoDB);
        var inputInfo = {
            bucket: _this.records[0].s3.bucket.name,
            key: _this.records[0].s3.object.key,
            schema: 'public',
            folderDepthLevelForTableName: 0,
            tableNames: [],
            connectionString: null,
            delimiter: ',',
            truncateTarget: false,
            tablePrefix: '',
            useSingleTable: false,
            sourceColumn: 'rds_loader_source_file',
            records: _this.records
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

Importer.prototype.run = function() {
	var _this = this;
    _this.getConfig()
        .then(_this.updateConfig)
        .then(_this.getTableNames)
        .then(_this.dropTablesIfExists)
        .then(_this.createTablesIfNotExists)
        .then(_this.runImport)
        .catch(function(error) {
            console.log(error.stack);
            context.done(error);
        })
        .finally(function() {
            pgp.end();
            context.done();
        });
};

module.exports = Importer;
