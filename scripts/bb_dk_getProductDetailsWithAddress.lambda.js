'use strict';

var AWS = require('aws-sdk');
var dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});

exports.handler = (event, context, callback) => {
    console.log('kvh =', event.kvh);
    
    var params = {
        "TableName": "bb_dk_address_mapping_dev",
        "KeyConditionExpression": "kvh=:val",
        "ExpressionAttributeValues": {":val": {"S": event.kvh}}
    };
    
    dynamodb.query(params, function(err, data) {
        if (err) {
            callback(null, {});
        } else {
            if (data.Count < 1) {
                callback(null, {});
            } else {
                var response = {
                    "kvh": data.Items[0].kvh.S,
                    "max_download_speed": data.Items[0].max_download_speed.N,
                    "max_upload_speed": data.Items[0].max_upload_speed.N
                };
    
                callback(null, response);
            }
        }
    });


};
