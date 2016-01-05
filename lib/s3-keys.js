/* Work around for 1000 file limitation on getObjects */
var _ = require('underscore');

function s3Keys(s3, maxKeys) {
	this.s3 = s3;
	this.maxKeys = maxKeys;
};

s3Keys.prototype.listKeys = function(options, callback) {
	var _this = this;
    var keys = [];
    function listKeysRecusively (marker) {
	    options.marker = marker;
        _this.listKeyPage(options,
        	function (error, nextMarker, keyset) {
				if (error) {
                	return callback(error, keys);
              	}

              	keys = keys.concat(keyset);

              	if (nextMarker) {
                	listKeysRecusively(nextMarker);
              	} else {
                	callback(null, keys);
              	}
          	});
    }
    // Start the recursive listing at the beginning, with no marker.
    listKeysRecusively();
};

s3Keys.prototype.listKeyPage = function(options, callback) {
	var _this = this;
	var params = {
		Bucket : options.bucket,
		Delimiter: options.delimiter,
		Marker : options.marker,
		MaxKeys : _this.maxKeys,
		Prefix : options.prefix
	};
     
    _this.s3.listObjects(params, function (error, response) {
    	if (error) {
    		return callback(error);
        } else if (response.err) {
        	return callback(new Error(response.err));
        }
     
        // Convert the results into an array of key strings, or
        // common prefixes if we're using a delimiter.
        var keys;
        if (options.delimiter) {
        	// Note that if you set MaxKeys to 1 you can see some interesting
          	// behavior in which the first response has no response.CommonPrefix
          	// values, and so we have to skip over that and move on to the 
			// next page.
          	keys = _.map(response.CommonPrefixes, function (item) {
            	return item.Prefix;
          	});
        } else {
        	keys = _.map(response.Contents, function (item) {
            	return item.Key;
          	});
        }
     
        // Check to see if there are yet more keys to be obtained, and if so
        // return the marker for use in the next request.
        var nextMarker;
        if (response.IsTruncated) {
        	if (options.delimiter) {
        		// If specifying a delimiter, the response.NextMarker field exists.
        		nextMarker = response.NextMarker;
          	} else {
          		// For normal listing, there is no response.NextMarker
            	// and we must use the last key instead.
            	nextMarker = keys[keys.length - 1];
            }
        }
     
        callback(null, nextMarker, keys);
    });
}

module.exports = s3Keys;