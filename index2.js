const Queue = require('bull');
const Processors = require('./processors');
require('dotenv').config();

// 6379
let importStixQueue = new Queue('importStix2', {redis: {port: 6500, host: 'localhost'}});

importStixQueue.add('importStix2',{
    apiRoot: 'apiroot1',
    collection: '9ee8a9b3-da1b-45d1-9cf6-8141f7039f82',
    file: 'big-dumb-test-file.json'
})
.then((result) => {
    console.log('added job: ', result)
});


importStixQueue.getJobCounts().then((result) => {
    console.log("job counts: ", result)
});