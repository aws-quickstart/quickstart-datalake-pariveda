var async = require('async');
var awsHelpers = require('aws-helpers');

exports.handler = (event, context, callback) => {
    var awsRegion = process.env.AWS_REGION;
    var firehoseStream = process.env.FirehoseStream;
    var outputStream = process.env.OutputStream;
	console.log('Received event:', JSON.stringify(event, null, 2));
    async.each(event.Records, function(record, callback){
        var data = new Buffer(record.kinesis.data, 'base64').toString('utf8');
        var updatedData = manipulateData(data);

        awsHelpers.sendDataToFirehose(awsRegion, firehoseStream, data, callback);

        // This is a random GUID
        var partitionKey = "cd37fb7e-6594-4e97-b109-dd9e8efa33d8";
        awsHelpers.sendDataToKinesis(awsRegion, outputStream, partitionKey, data, callback);
    },function(err){
        if(err) {
            console.log("Failed to process events.");
            context.fail("Failed to process events" + err);
        }
        else {
            console.log("Successfully processed " + event.Records.length + " records.");
            context.succeed("Successfully processed " + event.Records.length + " records.");
        }
    });
};

function manipulateData(data)
{
	return data;
}