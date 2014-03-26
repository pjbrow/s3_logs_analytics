///////////////////////////////////////////////////////////////////////////////////////////
// Data Setup
///////////////////////////////////////////////////////////////////////////////////////////

function Record(day, month, year, IpAddress, fileName) {
    this.day = day;
    this.month = month;
    this.year = year;
    this.IpAddress = IpAddress;
    this.fileName = fileName;
    this.stringifyRecord = function() {
        return this.day + "/" + this.month + "/" + this.year + ", " + "IP Address: " + this.IpAddress + " Filename: " + this.fileName + "\n";
    };
}
Record.prototype.toString = function recordToString() {
    return this.day + '{}' + this.month + '{}' + this.year + '{}' + this.IpAddress + '{}' + this.fileName;
};


///////////////////////////////////////////////////////////////////////////////////////////
// Setup AWS
///////////////////////////////////////////////////////////////////////////////////////////

var AWS = require('aws-sdk');
AWS.config.loadFromPath('config.json');
var s3 = new AWS.S3();
var params = {
    Bucket: 'alicepodcastlogs'
};


///////////////////////////////////////////////////////////////////////////////////////////
// Setup Other Dependencies
///////////////////////////////////////////////////////////////////////////////////////////

var _ = require('underscore');


///////////////////////////////////////////////////////////////////////////////////////////
// Get Data From AWS
///////////////////////////////////////////////////////////////////////////////////////////

function getListOfObjects() {
    s3.listObjects(params, function(err, data) {
        if (err) {
            console.log(err, err.stack); // an error occurred
        } else {
            console.log('Grabbing list of objects from Amazon.');
            var arrayOfObjectNames = data.Contents;
            return getAllObjects(arrayOfObjectNames);
        }
    });
}

function getAllObjects(arrayOfObjectNames) {
    // console.log(arrayOfObjectNames);
    var arrayLength = arrayOfObjectNames.length;
    var f = 0;
    var g = 0;
    var bigArray = [];
    console.log("Grabbing objects from generated object list.");
    arrayOfObjectNames.forEach(function() {
        var params2 = {
            Bucket: 'alicepodcastlogs',
            Key: arrayOfObjectNames[f].Key,
        };
        s3.getObject(params2, function(err, data) {
            var hit = data.Body.toString('utf8');
            bigArray.push(hit);
            g++;
            if (g === arrayLength - 1) {
                return processData(bigArray);
            }
        });
        f++;
    });
}


///////////////////////////////////////////////////////////////////////////////////////////
// Process Data
///////////////////////////////////////////////////////////////////////////////////////////

function processData(bigArray) {
    obArray = [];
    console.log('Cleaning up data.');
    for (e = 0; e < bigArray.length; e++) {
        var log = bigArray[e];
        var IpAddress = log.match(/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b/)[0];
        var date = log.match(/\[(.*?)\]/)[0];
        date = date.match(/\d\d\/\S\S\S\/\d\d\d\d/)[0];
        date = date.split('/');
        var fileName = log.match(/(REST.HEAD.OBJECT|REST.GET.OBJECT)(\s\S+)/) ? log.match(/(REST.HEAD.OBJECT|REST.GET.OBJECT)(\s\S+)/)[0].split(" ")[1] : 'not relevant';
        ob = new Record(date[0], date[1], date[2], IpAddress, fileName);
        obArray.push(ob);
    }
    return deleteDuplicates(obArray);
}

function deleteDuplicates(obArray) {
    var stringArray = [];
    console.log('Deleting duplicate data.');
    for (var q = 0; q < obArray.length; q++) {
        stringArray.push(obArray[q].toString());
    }
    return backToObjects(_.uniq(stringArray));
}

function backToObjects(arrayOfUniques) {
    var printArray = [];
    for (var k = 0; k < arrayOfUniques.length; k++) {
        var splitString = arrayOfUniques[k].split('{}');
        if (splitString[4].search('.mp3') > 0) {
            reconstitutedObject = new Record(parseInt(splitString[0]), splitString[1], splitString[2], splitString[3], splitString[4]);
            printArray.push(reconstitutedObject);
        }
    }
    return printToFile(printArray);
}

function printToFile(printArray) {
    var output = "";
    console.log(output);
    for (var h = 0; h < printArray.length; h++) {
        output = output + printArray[h].stringifyRecord();
    }
    require('fs').writeFile('file.txt', output);
    return mongoStash(printArray);
}


///////////////////////////////////////////////////////////////////////////////////////////
// Setup and Stash  Mongo
///////////////////////////////////////////////////////////////////////////////////////////

function mongoStash(printArray) {
    var MongoClient = require('mongodb').MongoClient,
        format = require('util').format;
    MongoClient.connect('mongodb://127.0.0.1:27017/aliPodcastAnalytics', function(err, db) {
        if (err) throw err;
        db.dropDatabase(function() {
            console.log('Resetting Database.');
        });
        var collection = db.collection('analytics_table');
        collection.insert(printArray, function(err, docs) {
            collection.aggregate([{
                $group: {
                    _id: "$day",
                    perDay: {
                        $sum: 1
                    }
                }
            }, {
                $sort: {
                    _id: 1
                }
            }], function(err, result) {
                console.log('Completed operation.');
                console.dir(result);
                db.close();
            });
        });
    });
}


///////////////////////////////////////////////////////////////////////////////////////////
// Run Script
///////////////////////////////////////////////////////////////////////////////////////////

getListOfObjects();
