var express = require("express");
var bodyParser = require("body-parser");
var path = require("path");
var AWS = require("aws-sdk");
AWS.config.update({
    region: "eu-west-1"
});
var fs = require('fs');
var dynamodb = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();


const create_tables = () => {
    var params = {
        TableName : "Movies",
        KeySchema: [       
            { AttributeName: "year", KeyType: "HASH"},  //Partition key
            { AttributeName: "title", KeyType: "RANGE" }  //Sort key
        ],
        AttributeDefinitions: [       
            { AttributeName: "year", AttributeType: "N" },
            { AttributeName: "title", AttributeType: "S" }
        ],
        ProvisionedThroughput: {       
            ReadCapacityUnits: 20, 
            WriteCapacityUnits: 20
        }
    };

    dynamodb.createTable(params, function(err, data) {
        if (err) {
            console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
        }
    });
}

const get_from_s3 = async() => {
    console.log("Getting data from S3 bucket.");
    const s3 = new AWS.S3(); // Pass in opts to S3 if necessary

    var params = {
    Bucket: 'csu44000assignment220',
    Key: 'moviedata.json'
    }

    const object = (await (s3.getObject(params).promise())).Body.toString('utf-8');
    return object;
}

const populate_tables = (movies) => {
    console.log("Importing movies into DynamoDB. Please wait.");

    var allMovies = JSON.parse(movies);
    let p = new Promise(resolve => {
        allMovies.forEach(function(movie) {
            var params = {
                TableName: "Movies",
                Item: {
                    "year":  movie.year,
                    "title": movie.title,
                    "info":  movie.info
                }
            };
    
            docClient.put(params, function(err, data) {
            if (err) {
                console.log("Insertion error.")
                // console.error("Unable to add movie", movie.title, ". Error JSON:", JSON.stringify(err, null, 2));
            } else {
                console.log("Insertion success.")
                // console.log("PutItem succeeded:", movie.title);
            }
            });
        });
    });
}

const create_db = async() => {
    let prom = await new Promise (resolve => {
        dynamodb.describeTable({TableName:"Movies"}, async function(err,result) { // Using describeTable to check if the table exists.
            if(!err){   // If describeTable doesn't throw an error, it means the table exists.
                resolve("Table already exists.");
            }
            else{       // If describeTable throws an error, the table does not exist, the function proceeds to create and populate the table.
                create_tables();
                // const movie_data = await get_from_s3();
                // await dynamodb.waitFor("tableExists", {TableName: "Movies"}).promise(); 
                // populate_tables(movie_data);
                resolve("Table was created and populated.");            
            }
        });
    });
    return prom;
};

const delete_db = async() => {
    let prom = await new Promise(resolve => {
        dynamodb.describeTable({TableName:"Movies"}, async function(err,result) { // Using describeTable to check if the table exists.
            if(err){   // If describeTable throws an error, it means the table does not exists.
                resolve("Table does not exist.");
            }
            else{       // If describeTable throws an error, the table exists, the function proceeds to delete the table.
                var params = {
                    TableName : "Movies"
                };
                dynamodb.deleteTable(params, function(err, data) {
                    if (err) {
                        console.error("Unable to delete table. Error JSON:", JSON.stringify(err, null, 2));
                        resolve("Table could not be deleted.");
                    } else {
                        resolve("Table was deleted.");
                    }
                });
            }
        });
    });
    return prom;
}

const query_db = (year, title_searched) => {    //add rating to query, allow query with null in one of the fields
    var response;
    console.log("Querying for movies from " + year + " with titles starting with " + title_searched);

    var params = {
        TableName : "Movies",
        ProjectionExpression:"#yr, title, info.genres, info.actors[0]",
        KeyConditionExpression: "#yr = :yyyy and begins_with (title, :title_searched)",
        ExpressionAttributeNames:{
            "#yr": "year"
        },
        ExpressionAttributeValues: {
            ":yyyy": year,
            ":title_searched": title_searched
        }
    };

    docClient.query(params, function(err, data) {
        if (err) {
            console.log("Unable to query. Error:", JSON.stringify(err, null, 2));
            response = "Query failed.";
        } else {
            console.log("Query succeeded.");
            data.Items.forEach(function(item) {
                console.log(" -", item.year + ": " + item.title + " ... " + item.info.genres
                + " ... " + item.info.actors[0]);
                response = " -", item.year + ": " + item.title + " ... " + item.info.genres
                + " ... " + item.info.actors[0];
            });
        }
    });
    return response;
}

const server = async() => {
    const port = 8080;
    var app = express();
    app.use(bodyParser.urlencoded({ extended: true }));
    app.use(bodyParser.json());

    let publicPath= path.resolve(__dirname,"public")
    app.use(express.static(publicPath))
    

    app.get('/', (req, res) => {
        res.sendFile('./movieclient.html', { root: __dirname });
    });
    
    app.get('/create', async(req, res) => {
        var response = await create_db();
        console.log(response);
        res.send(response);
    })

    app.get('/query', async(req, res) => {
        const year = parseInt(req.query.year);
        var response = await query_db(year, req.query.title);
        console.log(response);
        res.send(response);
    })

    app.get('/delete', async(req, res) => {
        var response = await delete_db();
        console.log(response);
        res.send(response);
    })
    
    var server = app.listen(port, function () {
       var host = server.address().address;
       var port = server.address().port;
       
       console.log("Example app listening at http://%s:%s", host, port);
    })
}

// create_db();
// delete_db();
// query_db(2013, "D");

server();