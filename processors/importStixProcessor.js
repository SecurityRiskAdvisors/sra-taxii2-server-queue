const JSONStream = require('JSONStream');
const mongoose = require('mongoose');
let es = require('event-stream');
const fs = require('fs');
const stix2Validator = require('sra-stix2-validator');
const ModelFactory = require('sra-taxii2-server-model/model-factory');
const MongoError = mongoose.mongo.MongoError;

const blockSize = 100;

/*
    Long Term: This could use threads/pooling https://www.npmjs.com/package/threads if needed
    Iterate through X number of records and batch them up.
    Then send that batch out to an async function that returns a promise which is sent out to a pool worker
    Repeat the last two steps until the pool is full and wait, then continue and repeat all until done.

    audit inserted data with time, source, etc
*/

const processStixItem = (data,es) => {
    let errors = stix2Validator.validate(data);
    if(errors !== null) {
        es.resume();
        return errors;
    } 

    // do additional validation/auditing?
    return null;
}

const getStix2InsertModel = async (apiroot, collection) => {
    let models = await ModelFactory.buildTaxii2Models(apiroot, collection, process.env.CONNECTION_STRING);
    return models.object;
}

const parseMongoWriteErrorArray = (errArray) => {
    let errReturn = [];

    let l = errArray.length;
    for(let i = 0; i < l; i++) {
        // do something else with this?
        errReturn.push(errArray[i].toString());
    }
    return errReturn
}

const parseResultData = (rawResult, type = null) => {
    let parsedResult = {
        errors : [],
        errorCount: 0,
        successCount : 0
    };

    if(rawResult instanceof MongoError && rawResult.name == "BulkWriteError") {
        if(rawResult.hasOwnProperty('writeErrors')) {
            parsedResult.errors = parseMongoWriteErrorArray(rawResult.writeErrors);
        }
        parsedResult.successCount = rawResult.result.nInserted;
    } else if(Object.prototype.toString.call(rawResult) === '[object Array]') {
        if(type == 'validation') {
            parsedResult.errors.push(...rawResult);
        } else {
            parsedResult.successCount += rawResult.length;
        }
    }
    parsedResult.errorCount = parsedResult.errors.length;
    return parsedResult;
};

const checkDataAndInsert = (insertBlock, data, errors) => {
    if(errors == null) {
        return insertBlock.pushData(data);
    } 
    
    return Promise.resolve(parseResultData(errors, 'validation'));
}


/* 
    @TODO - mixing async/await and promises a bit since async/await acts weird with streams,
    maybe look for a cleaner way
*/
const importStixProcessor = async(job) => {
    let model = await getStix2InsertModel(job.data.apiRoot, job.data.collection);

    job.progress(0);

    // @TODO - any way to avoid mutability with this obj? A lot of duplicated code otherwise
    let resultAggregator = {
        errors: [],
        successCount: 0,
        failureCount: 0,
        totalCount: 0,
        addError: function(error) {
            this.failureCount++;
            this.errors.push(error);
        },
        incorporateParsedResults: function(parsedResult) {
            this.errors.push(...parsedResult.errors);
            this.failureCount += parsedResult.errorCount;
            this.successCount += parsedResult.successCount;
        }
    };
    
    let insertBlock = {
        model: model,
        innerBlock: [],
        // referencing 'this' keyword in function, need to use full function syntax
        pushData: function(data, force = false) {
            this.innerBlock.push(data);

            return this.batchWriteBlock(force);
        },

        batchWriteBlock: function(force = false) {
            if(this.innerBlock.length >= blockSize || (force && this.innerBlock.length > 0)) {
                let insertBlockToDb = this.innerBlock;
                this.innerBlock = [];
                return model.insertMany(insertBlockToDb, {ordered: false});
            } else {
                return Promise.resolve(true);
            }
        }
    };
    
    let fileStream = fs.createReadStream(process.env.FILE_TEMP_DIR + '/' + job.data.file, {encoding: 'utf8'});

    let pipedStream = fileStream.pipe(JSONStream.parse('objects.*')).pipe(es.through(function (data) {
        this.pause();
        let validationErrors = processStixItem(data, this, model);
        
        checkDataAndInsert(insertBlock, data, validationErrors)
            .then((result) => {
                // if result is true, we're just waiting on insert block to fill up
                if(result !== true) {
                    resultAggregator.incorporateParsedResults(result);
                }
            }, (err) => {
                console.log("not implemented catch: ", err);
            })
            // Finally, resume the event stream and return
            .then(() => {
                resultAggregator.totalCount++;
                this.resume();
                return data;  
            });        
    }));

    const finalFunction = () => {
        return insertBlock.batchWriteBlock(true)
            .then((result) => {
                return Promise.resolve(parseResultData(result));
            }, (err) => {
                return Promise.resolve(parseResultData(err));
            })
            .then((parsedResult) => {
                resultAggregator.incorporateParsedResults(parsedResult);

                // match taxii spec for status endpoint
                // @TODO - make job->add() function use uuids to match TAXII spec for status
                let returnObj = {
                    status: 'complete',
                    failure_count: resultAggregator.failureCount,
                    success_count: resultAggregator.successCount,
                    total_count: resultAggregator.totalCount,
                    pending_count: 0,
                }

                job.progress(100);
                
                return Promise.resolve(returnObj);
            })
    }

    return new Promise(function(resolve, reject) {
        pipedStream.on('end', () => {
            // @TODO - this seems weird but it wasn't working just returning finalFunction() previously
            // I might be misunderstanding new Promise creation, research and clean up
            return finalFunction().then((result) => {
                resolve(result)
            });
        });
        pipedStream.on('error', (err) => reject(err));
        pipedStream.resume();
    });
}

module.exports = importStixProcessor