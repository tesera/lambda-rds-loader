/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var async = require('async');
require('./constants');

// function which creates a string representation of now suitable for use in S3
// paths
exports.getFormattedDate = function(date) {
	if (!date) {
		date = new Date();
	}

	var hour = date.getHours();
	hour = (hour < 10 ? "0" : "") + hour;

	var min = date.getMinutes();
	min = (min < 10 ? "0" : "") + min;

	var sec = date.getSeconds();
	sec = (sec < 10 ? "0" : "") + sec;

	var year = date.getFullYear();

	var month = date.getMonth() + 1;
	month = (month < 10 ? "0" : "") + month;

	var day = date.getDate();
	day = (day < 10 ? "0" : "") + day;

	return year + "-" + month + "-" + day + "-" + hour + ":" + min + ":" + sec;
};

/* current time as seconds */
exports.now = function() {
	return new Date().getTime() / 1000;
};

exports.readableTime = function(epochSeconds) {
	var d = new Date(0);
	d.setUTCSeconds(epochSeconds);
	return exports.getFormattedDate(d);
};

exports.createTables = function(dynamoDB, callback) {
	// processed files table spec
	var pfKey = 'loadFile';
	var configKey = s3prefix;
	var configSpec = {
		AttributeDefinitions : [ {
			AttributeName : configKey,
			AttributeType : 'S'
		} ],
		KeySchema : [ {
			AttributeName : configKey,
			KeyType : 'HASH'
		} ],
		TableName : configTable,
		ProvisionedThroughput : {
			ReadCapacityUnits : 5,
			WriteCapacityUnits : 5
		}
	};

	console.log("Creating Tables in Dynamo DB if Required");
	dynamoDB.createTable(configSpec, function(err, data) {
		if (err) {
			if (err.code !== 'ResourceInUseException') {
				console.log(Object.prototype.toString.call(err).toString());
				console.log(err.toString());
				process.exit(ERROR);
			}
		}
	});
};

exports.updateConfig = function(setRegion, dynamoDB, updateRequest, outerCallback) {
	var tryNumber = 0;
	var writeConfigRetryLimit = 100;

	async.whilst(function() {
		// retry until the try count is hit
		return tryNumber < writeConfigRetryLimit;
	}, function(callback) {
		tryNumber++;

		dynamoDB.updateItem(updateRequest, function(err, data) {
			if (err) {
				if (err.code === 'ResourceInUseException' || err.code === 'ResourceNotFoundException') {
					console.log(err.code);

					// retry if the table is in use after 1 second
					setTimeout(callback(), 1000);
				} else {
					// some other error - fail
					console.log(JSON.stringify(updateRequest));
					console.log(err);
					outerCallback(err);
				}
			} else {
				// all OK - exit OK
				if (data) {
					console.log("Configuration for " + updateRequest.Key.s3Prefix.S + " updated in " + setRegion);
					outerCallback(null);
				}
			}
		});
	}, function(error) {
		// never called
	});
};

exports.writeConfig = function(setRegion, dynamoDB, dynamoConfig, outerCallback) {
	var tryNumber = 0;
	var writeConfigRetryLimit = 100;

	async.whilst(function() {
		// retry until the try count is hit
		return tryNumber < writeConfigRetryLimit;
	}, function(callback) {
		tryNumber++;

		dynamoDB.putItem(dynamoConfig, function(err, data) {
			if (err) {
				if (err.code === 'ResourceInUseException' || err.code === 'ResourceNotFoundException') {
					// retry if the table is in use after 1 second
					setTimeout(callback(), 1000);
				} else {
					// some other error - fail
					console.log(JSON.stringify(dynamoConfig));
					console.log(JSON.stringify(err));
					if (outerCallback)
						outerCallback(err);
				}
			} else {
				// all OK - exit OK
				if (data) {
					console.log("Configuration for " + dynamoConfig.Item.s3Prefix.S + " successfully written in "
							+ setRegion);
					if (outerCallback)
						outerCallback(null);
				}
			}
		});
	}, function(error) {
		// never called
	});
};

exports.dropTables = function(dynamoDB, callback) {
	// drop the config table
	dynamoDB.deleteTable({
		TableName : configTable
	}, function(err, data) {
		if (err && err.code !== 'ResourceNotFoundException') {
			console.log(err);
			process.exit(ERROR);
		} else {
			console.log("All Configuration Tables Dropped");
			// call the callback requested
		}
	});
};

/* validate that the given value is a number, and if so return it */
exports.getIntValue = function(value, rl) {
	if (!value || value === null) {
		rl.close();
		console.log('Null Value');
		process.exit(INVALID_ARG);
	} else {
		var num = parseInt(value);

		if (isNaN(num)) {
			rl.close();
			console.log('Value \'' + value + '\' is not a Number');
			process.exit(INVALID_ARG);
		} else {
			return num;
		}
	}
};

exports.getBooleanValue = function(value) {
	if (value) {
		if ([ 'TRUE', '1', 'YES', 'Y' ].indexOf(value.toUpperCase()) > -1) {
			return true;
		} else {
			return false;
		}
	} else {
		return false;
	}
};

/* validate that the provided value is not null/undefined */
exports.validateNotNull = function(value, message, rl) {
	if (!value || value === null || value === '') {
		rl.close();
		console.log(message);
		process.exit(INVALID_ARG);
	}
};

/* turn blank lines read from STDIN to Null */
exports.blank = function(value) {
	if (value === '') {
		return null;
	} else {
		return value;
	}
};

exports.validateArrayContains = function(array, value, rl) {
	if (!(array.indexOf(value) > -1)) {
		rl.close();
		console.log('Value must be one of ' + array.toString());
		process.exit(INVALID_ARG);
	}
};

exports.createManifestInfo = function(config) {
	// manifest file will be at the configuration location, with a fixed
	// prefix and the date plus a random value for uniqueness across all
	// executing functions
	var dateName = exports.getFormattedDate();
	var rand = Math.floor(Math.random() * 10000);

	var manifestInfo = {
		manifestBucket : config.manifestBucket.S,
		manifestKey : config.manifestKey.S,
		manifestName : 'manifest-' + dateName + '-' + rand
	};
	manifestInfo.manifestPrefix = manifestInfo.manifestKey + '/' + manifestInfo.manifestName;
	manifestInfo.manifestPath = manifestInfo.manifestBucket + "/" + manifestInfo.manifestPrefix;

	return manifestInfo;
};

exports.randomInt = function(low, high) {
	return Math.floor(Math.random() * (high - low) + low);
};