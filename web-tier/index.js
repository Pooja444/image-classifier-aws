// we use express and multer libraries to send images
const express = require('express');
const multer = require('multer');
const fs = require('fs');
const https = require('https');
const AWS = require('aws-sdk');
const { Consumer } = require('sqs-consumer');
const { v4: uuidv4 } = require('uuid');

require('dotenv').config()

const penv = process.env

// uploaded images are saved in the folder "/upload_images"
const upload = multer({ dest: __dirname + '/upload_images' });

const s3 = new AWS.S3({
    accessKeyId: penv.ACCESS_KEY_ID,
    secretAccessKey: penv.SECRET_ACCESS_KEY
});

AWS.config.update({ accessKeyId: penv.ACCESS_KEY_ID, secretAccessKey: penv.SECRET_ACCESS_KEY, region: penv.AWS_REGION });
var sqs = new AWS.SQS({ apiVersion: '2012-11-05' });

// Maps promises and a JSON that contains the resolver function and the reject function
var PROMISES = {}

// A generic executor. 
// We use this to add resolve and reject functions to the PROMISES map
const executor = (resolve, reject, id) => {
    // Generic resolver. 
    PROMISES[id] = {
        resolve,
        reject
    }
}

// This is an SQS consumer that will listen to the response queue for responses every 500 milliseconds
const consumerApp = Consumer.create({
    queueUrl: penv.RESPONSE_QUEUE_URL,
    pollingWaitTimeMs: 500,
    handleMessage: async (message) => {

	// Use the message id to resolve the corresponding promise and delete it from the promises map
        const messageBody = JSON.parse(message['Body']);
        messageBody['timestamps']['webtier']['response'] = new Date().getTime();
        const id = messageBody['id'];
        const fns = PROMISES[id]
        delete PROMISES[id]
        fns.resolve(messageBody['result'])
    },
    sqs: new AWS.SQS({
        httpOptions: {
            agent: new https.Agent({
                keepAlive: true
            })
        }
    })
});

consumerApp.on('error', (err) => {
    console.error(err.message);
});

consumerApp.on('processing_error', (err) => {
    console.error(err.message);
});

consumerApp.start()


// Server that will listen to http requests
const server = express();
server.use(express.static('public'));

// "myfile" is the key of the http payload
server.post('/', upload.single('myfile'), function (request, response) {
    response.status(200)

    // save the image
    var fs = require('fs');
    fs.rename(__dirname + '/upload_images/' + request.file.filename, __dirname + '/upload_images/' + request.file.originalname, function (err) {
        if (err) console.log('ERROR: ' + err);
    });

    // upload image to s3 bucket
    uploadFile(request.file.originalname);

    // delete image from upload_images folder
    fs.unlinkSync(__dirname + '/upload_images/' + request.file.originalname);
    
    // send message to request queue
    const requestTime = new Date().getTime();
    const id = uuidv4();
    var params = {
        MessageBody: JSON.stringify({ "id": id, "s3_key": request.file.originalname, "timestamps": { "webtier": { "request": requestTime } } }),
        QueueUrl: penv.REQUEST_QUEUE_URL
    };
    sqs.sendMessage(params, function (err, data) {
        if (err) {
            console.log("Error", err);
        } else {
            console.log("Success", data.MessageId);
        }
    });

    const p = new Promise((resolve, reject) => executor(resolve, reject, id))

    // We freeze here till we get a response
    p.then((v) => {
	console.log("Sending response: " + v)
        
	// Receives value from the `resolve`
        response.send(v)
    }).catch(() => {
        response.send("Couldn't classify")
    })
});

const uploadFile = (fileName) => {
    // Read content from the file
    const fileContent = fs.readFileSync('upload_images/' + fileName);

    // Setting up S3 upload parameters
    const params = {
        Bucket: penv.S3_BUCKET_NAME,
        Key: fileName, // File name you want to save as in S3
        Body: fileContent
    };

    // Uploading files to the bucket
    s3.upload(params, function (err, data) {
        if (err) {
            throw err;
        }
        console.log(`File uploaded successfully. ${data.Location}`);
    });
};

const hostname = '0.0.0.0';
app = server.listen(penv.PORT, hostname, () => {
    console.log(`Server running at http://${hostname}:${penv.PORT}/`);
}).setTimeout(420000);
