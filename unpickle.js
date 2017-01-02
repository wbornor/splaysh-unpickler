/**
 * Created by wesbornor on 1/1/17.
 */
var AWS = require('aws-sdk');
var fs = require('fs');
var dateFormat = require('dateformat');

AWS.config.setPromisesDependency(require('bluebird'));

var INPUT = '/Users/wesbornor/git/splaysh-unpickler/src/main/resources/splayshdb-all.json';
var TABLE = 'splayshdb.dev.items';

AWS.config.update({
    region: "us-east-1"
});

var dynamoDb = new AWS.DynamoDB.DocumentClient();


var allItems = JSON.parse(fs.readFileSync(INPUT, 'utf8'));
allItems.forEach(function (item) {
    var params = {
        TableName: TABLE,
        Item: {}
    };

    for (var name in item) {
        if (item.hasOwnProperty(name)) {
            if (item[name]) {
                var value = item[name];
                if (name === 'id') {
                    value = value + '';
                }
                params.Item[name] = value;
            }
        }
    }

    if (!item.hasOwnProperty('create_date')) {
        if (typeof item.id === 'number') {
            var d;
            d = new Date('2006-06-28 0:0:0');
            d.setSeconds(d.getSeconds() + item.id);
            params.Item.create_date = dateFormat(d, "yyyy-mm-dd h:MM:ss");
        }
    }

    //TODO enrich w/ `nut_type` if missing


    var putPromise = dynamoDb.put(params).promise();
    putPromise.then(function (result) {
       //console.log("dynamodb put succeeded: " + JSON.stringify(result));
    }).catch(function (err) {
        // handle error
        console.error('error: id: ' + item.id + " content: " + item.content + " item: " + JSON.stringify(item) + " **** Unable to put" + JSON.stringify(err));
    });
});

console.log("unpickle time");

