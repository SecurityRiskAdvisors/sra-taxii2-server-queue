const Queue = require('bull');
const Processors = require('./processors');
require('dotenv').config();

// 6379
let importStixQueue = new Queue('importStix2', {redis: {port: 6379, host: 'sra-taxii2-redis'}});

console.log("job queue started...");
importStixQueue.process('importStix2', 10, Processors.importStixProcessor);

importStixQueue.on('completed', function(job, result){
    console.log("completed job: ", job);
    console.log(" job result: ", result);
});
