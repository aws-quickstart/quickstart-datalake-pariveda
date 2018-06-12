var async = require('async');
var awsHelpers = require('aws-helpers');

exports.handler = (event, context, callback) => {
	console.log('Received event:', JSON.stringify(event, null, 2));
    var startDate = new Date(Date.parse(event.startDate));
    var endDate = new Date(Date.parse(event.endDate));
    console.log('Searching for data between %s and %s', startDate.toISOString(), endDate.toISOString());
    readData(startDate, endDate, callback);
};


function readData(startDate, endDate, callback)
{
    var awsRegion = process.env.AWS_REGION;
    var bucketName = process.env.OutputBucketName;
    var prefixes = getFirehosePrefixes(startDate, endDate);
    var getData = function(prefix, callback){
        readObjectsUnderPrefix(awsRegion, bucketName, prefix, startDate, endDate, callback); 
    }
    async.map(prefixes, getData, function(err, results) {
        if (err){
            console.log(err, err.stack);
            callback(err);
        }else{
            console.log('Read data: %s', results);
            var combinedResult = [].concat.apply([], results);
            callback(null, combinedResult);
        }
    });
}

function getFirehosePrefixes(startDate, endDate)
{
    var basePrefix = process.env.ObjectPrefix;

    var hours = getHoursBetweenDates(startDate, endDate);
    var prefixes = [];
    for(var i = 0; i < hours.length; i++){
        var prefix = getPrefixForDate(basePrefix, hours[i]);
        prefixes.push(prefix);
    }
    return prefixes;
}

function readObjectsUnderPrefix(awsRegion, bucketName, prefix, startDate, endDate, callback){
    console.log('Reading data from Region: %s:, Bucket: %s, Prefix: %s.', awsRegion, bucketName, prefix);

    awsHelpers.listAllS3Objects(awsRegion, bucketName, prefix, [], null, function(allS3Objects){
        console.log('Found %d items total', allS3Objects.length);
        var filteredObjects = filterObjectsByTimeRange(allS3Objects, startDate, endDate);
        console.log('Found %d items in date range', filteredObjects.length);
        awsHelpers.readAllS3Objects(awsRegion, bucketName, filteredObjects, callback);
    })
}

function filterObjectsByTimeRange(s3Objects, startDate, endDate)
{
    console.log('Finding objects between %s and %s', startDate.toISOString(), endDate.toISOString());
    var filteredObjects = s3Objects.filter(s3Object =>{
        var key = s3Object.Key;
        var regx = /\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}/
        var dateString = key.match(regx).pop();
        var dateParts = dateString.split("-");
        console.log('Date parts: %s', dateParts);
        var s3ObjectDate = new Date(parseInt(dateParts[0], 10),
                                    // Months are zero based
                                    parseInt(dateParts[1], 10) - 1,
                                    parseInt(dateParts[2], 10),
                                    parseInt(dateParts[3], 10),
                                    parseInt(dateParts[4], 10),
                                    parseInt(dateParts[5], 10));
        console.log('Found object with date %s', s3ObjectDate.toISOString());
        return(startDate <= s3ObjectDate) && (s3ObjectDate <= endDate);
    });
    return filteredObjects;
}

function getHoursBetweenDates(startDate, stopDate) {
    var hours = new Array();
    var currentDate = new Date(startDate.getTime());;
    while (currentDate <= stopDate) {
        hours.push(new Date (currentDate));
        currentDate.setHours(currentDate.getHours()+1);
    }
    return hours;
}

function pad(n) {
    return (n < 10) ? ("0" + n) : n;
}

function getPrefixForDate(basePrefix, date){
    var year = date.getFullYear();
    var month = date.getMonth()+1;
    var day = date.getDate();
    var hour = date.getHours();
    return `${basePrefix}/${year}/${pad(month)}/${pad(day)}/${pad(hour)}/`;
}