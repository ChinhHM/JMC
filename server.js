var express = require('express'),
    app = express(),
    bodyParser = require('body-parser'),
    MongoClient = require('mongodb').MongoClient,
    engines = require('consolidate'),
    assert = require('assert'),
    ObjectId = require('mongodb').ObjectID;

var url = '';
//var url = 'mongodb://jmcdb01:btF60bUrXKjcVSVBRnaUBFR34VuCqkgQYDngvDWglG7GchYaG3lWdwfTGz17p73tXz2fFj3qTULPNanHUpdcZQ==@jmcdb01.documents.azure.com:10255/?ssl=true&replicaSet=globaldb';

// Application Insights initialization
const appInsights = require("applicationinsights");

// Get AppInsights Instrumentation key via environment variable
appInsights.setup(process.env.APPINSIGHTS_KEY); // db1e8225-31cb-42b9-9e60-429ad461fb14
//appInsights.setup('db1e8225-31cb-42b9-9e60-429ad461fb14');
appInsights.start();
// Application Insights initialization

//Pagination init
const paginate = require('express-paginate');
app.use(paginate.middleware(10, 50));

//Key vault variables
var msRestAzure = require('ms-rest-azure');
var KeyVault = require('azure-keyvault');
var AuthenticationContext = require('adal-node').AuthenticationContext;

/*var clientId = 'f23d42d6-f2eb-452a-aea7-6b0b8cbd86cc'; // service principal
var domain = '72f988bf-86f1-41af-91ab-2d7cd011db47'; // tenant id
var secret = 'lO8FkYsWqU9G2YzxPe1m3CoYjRdMPZAOU0YwbswABUc=';
var keyVaultSecretName = 'CUSTOMCONNSTRToCosmosDB';
var vaultName = 'jmckv';*/

var clientId = process.env.CLIENTID;
var domain = process.env.TENANTID;
var secret = process.env.CLIENTSECRET;
var keyVaultSecretName = process.env.VAULTSECRETNAME;
var vaultName = process.env.VAULTNAME;

var version = ''; 
var keyVaultClient;
var vaultUri = `https://${vaultName}.vault.azure.net/`;

// Redis cache
var cacheEnabled = 1;
//var cacheEnabled = 0;
var redis = require('redis');

/*var RedisURL = 'jmccache.redis.cache.windows.net';
var RedisKeyName = 'RedisKey';
var RedisKey = 'NvWvMg+nJ1fSgPZbftAVmLVje4kBN8VyBW771GaRIug=';*/

var RedisURL = process.env.REDISURL;
//var RedisKey = process.env.REDISKEY;
var RedisKey = '';
var RedisKeyName = process.env.REDISKEYNAME;

var RedisClient;
var bluebird = require('bluebird');

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

// Express initialization
app.use(express.static(__dirname + "/public"));

app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());

app.engine('html', engines.nunjucks);
app.set('view engine', 'html');
app.set('views', __dirname + '/views');

function errorHandler(err, req, res, next) {
    console.error(err.message);
    console.error(err.stack);
    res.status(500).render("error_template", { error: err});
}

// Authenticate using ADAL node
function authenticator(challenge, callback) {
    // Create a new authentication context.
    var context = new AuthenticationContext(challenge.authorization);

    // Use the context to acquire an authentication token.
    return context.acquireTokenWithClientCredentials(challenge.resource, clientId, secret, function (err, tokenResponse) {
        if (err) throw err;
        // Calculate the value to be set in the request's Authorization header and resume the call.
        var authorizationValue = tokenResponse.tokenType + ' ' + tokenResponse.accessToken;
        return callback(null, authorizationValue);
    });
}

async function main(){
    // Authenticate to key vault and get secret (connection string to CosmosDB)
    try {
        // Login with the service principal created for jmcapp01
        var init = await msRestAzure.loginWithServicePrincipalSecret(clientId, secret, domain);

        var kvCredentials = new KeyVault.KeyVaultCredentials(authenticator);
        keyVaultClient = new KeyVault.KeyVaultClient(kvCredentials);
    
        // get the secret's value from key vault
        try {
            // Get connection string to CosmosDB
            var resultGetSecret = await keyVaultClient.getSecret(vaultUri, keyVaultSecretName, version);
            console.log("Connection string =  " + JSON.stringify(resultGetSecret.value));
            url = resultGetSecret.value;

            // Get Redis key 
            resultGetSecret = await keyVaultClient.getSecret(vaultUri, RedisKeyName, version);
            console.log("Redis key = " + JSON.stringify(resultGetSecret.value));
            RedisKey = resultGetSecret.value;
        } finally {
        }
    } catch (err) {
        console.log(err);
    }

    // Connect to Redis cache
    try {
        RedisClient = redis.createClient(6380, RedisURL, {auth_pass: RedisKey, tls: {servername: RedisURL}});
        await RedisClient.flushallAsync();
    } catch (err) {
        Console.log(err);
    }

    // Connect to CosmosDB using the retrieved connection string
    MongoClient.connect(url, function(err, db){
        assert.equal(null, err);
        console.log('Successfully connected to MongoDB.');
    
        var records_collection = db.collection('records');
        var noOfRecords, pageCount, cacheResult, nCurrentPage;
        var bFlushLastPage = false;
        var nRecordsPerPage;
        var nPageToRefresh = 0;
    
        app.get('/records', async function(req, res, next) {
            // console.log("Received get /records request");
            // Query only the records on current page
            nCurrentPage = req.query.page;
            if (cacheEnabled) {
                //console.log("pageCount = " + pageCount + " page requested = " + nCurrentPage + " flush =" + bFlushLastPage + " no of records = " + noOfRecords + " page to refresh = " + nPageToRefresh);
                // If current page is not the last page, and bFlushLastPage is not flagged (i.e, no new record added)
                // and the current page is not updated -> query cache
                if (!((pageCount === req.query.page)&&(bFlushLastPage)) && (nCurrentPage != nPageToRefresh)) {
                    cacheResult = await RedisClient.getAsync(req.query.page);
                    if (cacheResult) {
                        console.log("Cache hit, page = " + req.query.page);
                        //console.log("Cached result = " + cacheResult);
                        bFlushLastPage = false; // reset last page flag
                        nPageToRefresh = 0; // reset page to refresh
                        return res.json(JSON.parse(cacheResult));
                    }
                }
            }
            
            console.log("Cache missed, querying DB");
            results = records_collection.find({}).limit(req.query.limit).skip(req.skip);
            records_collection.count({}, function(error, noOfDocs){
                if (error) console.log(error.message);
                
                noOfRecords = noOfDocs;
                pageCount = Math.ceil(noOfRecords / req.query.limit);
                nRecordsPerPage = req.query.limit;
            });
            
            results.toArray(async function(err, records){
                if(err) throw err;
    
                if(records.length < 1) {
                    console.log("No records found.");
                }
    
                // console.log(records);
                await RedisClient.set(req.query.page, JSON.stringify({
                    recs: records,
                    pgCount: pageCount,
                    itemCount: noOfRecords
                    }));

                res.json({
                    recs: records,
                    pgCount: pageCount,
                    itemCount: noOfRecords
                    //pages: paginate.getArrayPages(req)(3, pageCount, req.query.page)
                });
            });
        });
    
        app.post('/records', function(req, res, next){
            console.log(req.body);
            records_collection.insert(req.body, async function(err, doc) {
                if(err) throw err;
                console.log(doc);

                // clear cache
                //console.log("DB changed, clearing cache!");
                //await RedisClient.flushall();
                
                // Flush cache strategy:
                // - If current page is last page, check if the page is cached, if yes, flag the page to be invalidated. On the next GET request, we will need to bypass the cache
                // - If the added record belongs to new page, notify the server to bypass cache for the next GET (we need to query DB to get new number of records and pages)
                noOfRecords++;
                var nNewPageCount = Math.ceil(noOfRecords / nRecordsPerPage);
                if (nNewPageCount === pageCount) { // record added on the same page
                    pageCount = nNewPageCount;
                    var reply = await RedisClient.existsAsync(pageCount);
                    if (reply === 1) {
                        bFlushLastPage = true;
                        console.log("last page, pageCount = " + pageCount);
                    }
                }
                else { // record added on new page
                    bFlushLastPage = true;
                }
                
                res.json(doc);
            });
        });
    
        app.delete('/records/:id', function(req, res, next){
            var id = req.params.id;
            console.log("delete " + id);
            records_collection.deleteOne({'_id': new ObjectId(id)}, async function(err, results){
                console.log(results);

                // clear cache
                console.log("DB changed, clearing cache!");
                await RedisClient.flushall();

                res.json(results);
            });
        });
    
        app.put('/records/:id', function(req, res, next){
            var id = req.params.id;
            records_collection.updateOne(
                {'_id': new ObjectId(id)},
                { $set: {
                    'name' : req.body.name,
                    'email': req.body.email,
                    'phone': req.body.phone
                    }
                }, async function(err, results){
                    console.log(results);

                    // clear cache
                    //console.log("DB changed, clearing cache!");
                    //await RedisClient.flushall();

                    // Flush cache strategy:
                    // - Notify server to bypass cache for current page
                    nPageToRefresh = nCurrentPage;

                    res.json(results);
            });
        });
    
        app.use(errorHandler);
        var server = app.listen(process.env.PORT || 3000, function() {
            var port = server.address().port;
            console.log('Express server listening on port %s.', port);
        })
    });
}

// main function
main();
