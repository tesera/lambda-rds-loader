/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

/**
 * Ask questions of the end user via STDIN and then setup the DynamoDB table
 * entry for the configuration when done
 */
var pjson = require('./package.json');
var readline = require('readline');
var aws = require('aws-sdk');
require('./lib/constants');
var common = require('./lib/common');
var async = require('async');
var uuid = require('node-uuid');
var dynamoDB;
var kmsCrypto = require('./lib/kmsCrypto');
var setRegion;

dynamoConfig = {
	TableName : configTable,
	Item : {
		currentBatch : {
			S : uuid.v4()
		},
		version : {
			S : pjson.version
		},
		loadRDS : {
			L : [ {
				M : {

				}
			} ]
		}
	}
};

/* configuration of question prompts and config assignment */
var rl = readline.createInterface({
	input : process.stdin,
	output : process.stdout
});

var qs = [];

q_region = function(callback) {
	rl.question('Enter the Region for the Configuration > ', function(answer) {
		if (common.blank(answer) !== null) {
			common.validateArrayContains([ "ap-northeast-1", "ap-southeast-1", "ap-southeast-2", "eu-central-1", "eu-west-1", "sa-east-1", "us-east-1", "us-west-1", "us-west-2" ],
					answer.toLowerCase(), rl);

			setRegion = answer.toLowerCase();

			// configure dynamo db and kms for the correct region
			dynamoDB = new aws.DynamoDB({
				apiVersion : '2012-08-10',
				region : setRegion
			});
			kmsCrypto.setRegion(setRegion);

			callback(null);
		}
	});
};

q_s3Prefix = function(callback) {
	rl.question('Enter the S3 Bucket > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide an S3 Bucket Name', rl);

		// setup prefix to be * if one was not provided
		var stripped = answer.replace(new RegExp('s3://', 'g'), '');
		var elements = stripped.split("/");
		var setPrefix = undefined;

		if (elements.length === 1) {
			// bucket only so use "bucket" alone
			setPrefix = elements[0];
		} else {
			// right trim "/"
			setPrefix = stripped.replace(/\/$/, '');
		}

		dynamoConfig.Item.s3Prefix = {
			S : setPrefix
		};

		callback(null);
	});
};

q_filenameFilter = function(callback) {
	rl.question('Enter a Filename Filter Regex > ', function(answer) {
		if (common.blank(answer) !== null) {
			dynamoConfig.Item.filenameFilterRegex = {
				S : answer
			};
		}
		callback(null);
	});
};

q_rdsHost = function(callback) {
	rl.question('Enter the RDS Host > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide an RDS Host', rl);
		dynamoConfig.Item.loadRDS.L[0].M.rdsHost = {
			S : answer
		};
		callback(null);
	});
};

q_rdsPort = function(callback) {
	rl.question('Enter the RDS Port > ', function(answer) {
		dynamoConfig.Item.loadRDS.L[0].M.rdsPort = {
			N : '' + common.getIntValue(answer, rl)
		};
		callback(null);
	});
};

q_rdsDB = function(callback) {
	rl.question('Enter the Database Name > ', function(answer) {
		if (common.blank(answer) !== null) {
			dynamoConfig.Item.loadRDS.L[0].M.rdsDB = {
				S : answer
			};
		}
		callback(null);
	});
};

q_userName = function(callback) {
	rl.question('Enter the Database Username > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Username', rl);
		dynamoConfig.Item.loadRDS.L[0].M.connectUser = {
			S : answer
		};
		callback(null);
	});
};

q_userPwd = function(callback) {
	rl.question('Enter the Database Password > ', function(answer) {
		common.validateNotNull(answer, 'You Must Provide a Password', rl);

		kmsCrypto.encrypt(answer, function(err, ciphertext) {
			if (err) {
				console.log(JSON.stringify(err));
				process.exit(ERROR);
			} else {
				dynamoConfig.Item.loadRDS.L[0].M.connectPassword = {
					S : kmsCrypto.toLambdaStringFormat(ciphertext)
				};
				callback(null);
			}
		});
	});
};

q_schema = function(callback) {
	rl.question('Enter the Schema (optional) > ', function(answer) {
		if (answer && answer !== null && answer !== "") {
			dynamoConfig.Item.loadRDS.L[0].M.targetSchema = {
				S : answer
			};
			callback(null);
		} else {
			callback(null);
		}
	});
};

q_tablePrefix = function(callback) {
	rl.question('Enter table prefix (optional but recommended if keys start with numericals) > ', function(answer) {
		if (answer && answer !== null && answer !== "") {
			dynamoConfig.Item.loadRDS.L[0].M.tablePrefix = {
				S : answer
			};
			callback(null);
		} else {
			callback(null);
		}
	});
};

q_folderDepthLevelForTableName = function(callback) {
	rl.question('Enter the folder depth from bucket root to use as table name. Use negative index to select from the input file > ', function(answer) {
		dynamoConfig.Item.folderDepthLevelForTableName = {
			N : '' + common.getIntValue(answer, rl)
		};
		callback(null);
	});
};

q_truncateTable = function(callback) {
	rl.question('Should the Table be Truncated before Load? (Y/N) > ', function(answer) {
		dynamoConfig.Item.loadRDS.L[0].M.truncateTarget = {
			BOOL : common.getBooleanValue(answer)
		};
		callback(null);
	});
};

q_csvDelimiter = function(callback) {
	//if (dynamoConfig.Item.dataFormat.S === 'CSV') {
		rl.question('Enter the CSV Delimiter > ', function(answer) {
			common.validateNotNull(answer, 'You Must the Delimiter for CSV Input', rl);
			dynamoConfig.Item.csvDelimiter = {
				S : answer
			};
			callback(null);
		});
	//} else {
	//	callback(null);
	//}
};

last = function(callback) {
	rl.close();

	setup(null, callback);
};

q_useSingleTable = function(callback) {
	rl.question('Should the load utilize a single table for loads? (Y/N) > ', function(answer) {
		dynamoConfig.Item.loadRDS.L[0].M.useSingleTable = {
			BOOL : common.getBooleanValue(answer)
		};
		callback(null);
	});
};

setup = function(overrideConfig, callback) {
	// set which configuration to use
	var useConfig = undefined;
	if (overrideConfig) {
		useConfig = overrideConfig;
	} else {
		useConfig = dynamoConfig;
	}
	var configWriter = common.writeConfig(setRegion, dynamoDB, useConfig, callback);
	common.createTables(dynamoDB, configWriter);
};
// export the setup module so that customers can programmatically add new
// configurations
exports.setup = setup;

qs.push(q_region);
qs.push(q_s3Prefix);
qs.push(q_filenameFilter);
qs.push(q_rdsHost);
qs.push(q_rdsPort);
qs.push(q_rdsDB);
qs.push(q_schema);
qs.push(q_tablePrefix);
qs.push(q_useSingleTable);
qs.push(q_folderDepthLevelForTableName);
qs.push(q_truncateTable);
qs.push(q_userName);
qs.push(q_userPwd);
qs.push(q_csvDelimiter);

// always have to have the 'last' function added to halt the readline channel
// and run the setup
qs.push(last);

// call the first function in the function list, to invoke the callback
// reference chain
async.waterfall(qs);
