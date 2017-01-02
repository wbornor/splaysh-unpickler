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
    if (!item.hasOwnProperty('nut_type')) {
        var type = "TALKNUT";
        if (typeof item.nut_id === 'number') {
            switch (item.nut_id) {
                case 1:
                    type = "TALKNUT";
                    break;
                case 2:
                    type = "FAVNUT";
                    break;
                case 3:
                    type = "PHOTONUT";
                    break;
                case 4:
                    type = "WISHNUT";
                    break;
                // case 5:
                //     type = "FAVNUT";
                //     break;
                case 6:
                    type = "MEDIANUT";
                    break;
                case 7:
                    type = "PROJECTNUT";
                    break;
                case 8:
                    type = "ANALNUT";
                    break;
                case 9:
                    type = "VIDINUT";
                    break;
                case 10:
                    type = "AUDINUT";
                    break;
                // case 11:
                //     type = "FAVNUT";
                //     break;
                case 12:
                    type = "BUDNUT";
                    break;
                case 13:
                    type = "MAPNUT";
                    break;
                default:
                    type = "TALKNUT";
                    break;
            }
            var d;
            d = new Date('2006-06-28 0:0:0');
            d.setSeconds(d.getSeconds() + item.id);
            params.Item.create_date = dateFormat(d, "yyyy-mm-dd h:MM:ss");
        }

        params.Item.nut_type = type;
    }


    var putPromise = dynamoDb.put(params).promise();
    putPromise.then(function (result) {
        console.log("dynamodb put succeeded: " + JSON.stringify(result));
    }).catch(function (err) {
        // handle error
        console.error('error: id: ' + item.id + " content: " + item.content + " item: " + JSON.stringify(item) + " **** Unable to put" + JSON.stringify(err));
    });


});

console.log("unpickle time");

