var express = require("express");
var bodyParser = require("body-parser");
var path = require("path");
var AWS = require("aws-sdk");
AWS.config.update({
    region: "eu-west-1"
});
var dynamodb = new AWS.DynamoDB();
var docClient = new AWS.DynamoDB.DocumentClient();


const create_tables = () => {   //Creates Movie table, with year and title as key.
    var params = {
        TableName : "Movies",
        KeySchema: [       
            { AttributeName: "year", KeyType: "HASH"},
            { AttributeName: "title", KeyType: "RANGE" }
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

const get_from_s3 = async() => {    //Returns with movie data from S3 Bucket.
    console.log("Getting data from S3 bucket.");
    const s3 = new AWS.S3();

    var params = {
    Bucket: 'csu44000assignment220',
    Key: 'moviedata.json'
    }

    const object = (await (s3.getObject(params).promise())).Body.toString('utf-8');
    return object;
}

const populate_tables = (movies) => {
    let prom;
    console.log("Importing movies into DynamoDB. Please wait.");

    var allMovies = JSON.parse(movies);
    allMovies.forEach(function(movie) {
        var params = {
            TableName: "Movies",
            Item: {
                "year":  movie.year,
                "title": movie.title,
                "rating":  movie.info.rating,
                "genre": movie.info.genres
            }
        };

        prom = docClient.put(params, function(err, data) {
            if (err) {
                console.error("Unable to add movie", movie.title, ". Error JSON:", JSON.stringify(err, null, 2));
            } 
        }).promise().catch(error => console.log('error', error));
    });

    return prom;    //This data is not used, but returning a promise allows async await to be used on this function.
}

const create_db = async() => {  //Checks if db already exists, if not calls create_tables and populate_tables. Returns response to send to client.
    let prom = await new Promise (resolve => {
        dynamodb.describeTable({TableName:"Movies"}, async function(err,result) { // Using describeTable to check if the table exists.
            if(!err){   // If describeTable doesn't throw an error, it means the table exists.
                resolve("Table already exists.");
            }
            else{       // If describeTable throws an error, the table does not exist.
                create_tables();
                const movie_data = await get_from_s3();
                await dynamodb.waitFor("tableExists", {TableName: "Movies"}).promise(); 
                await populate_tables(movie_data);
                resolve("Table was created and populated.");            
            }
        });
    }).catch(error => console.log('error', error));
    return prom;
};

const delete_db = async() => {  //Checks if db exists, if so deletes the table. Returns response to send to client.
    let prom = await new Promise(resolve => {
        dynamodb.describeTable({TableName:"Movies"}, async function(err,result) { // Using describeTable to check if the table exists.
            if(err){   // If describeTable throws an error, it means the table does not exists.
                resolve("Table does not exist.");
            }
            else{       // If describeTable throws an error, the table exists.
                var params = {
                    TableName : "Movies"
                };
                dynamodb.deleteTable(params, async function(err, data) {
                    if (err) {
                        console.error("Unable to delete table. Error JSON:", JSON.stringify(err, null, 2));
                        resolve("Table could not be deleted.");
                    } else {
                        console.log("Deleting table")
                        await dynamodb.waitFor("tableNotExists", {TableName: "Movies"}).promise();
                        resolve("Table was deleted.");
                    }
                });
            }
        });
    }).catch(error => console.log('error', error));
    return prom;
}

const query_db = (year, title_searched, rating_searched) => {   //Sends query to DynamoDB. Returns array of movies that satify it.
    console.log("Querying for movies from " + year + " with titles starting with " 
        + title_searched + " and ratings greater than " + rating_searched);

    var params = {
        TableName : "Movies",
        ProjectionExpression:"#yr, title, rating, genre",
        KeyConditionExpression: "#yr = :yyyy and begins_with (title, :title_searched)",
        FilterExpression: "rating >= :rating_searched",
        ExpressionAttributeNames:{
            "#yr": "year"
        },
        ExpressionAttributeValues: {
            ":yyyy": year,
            ":title_searched": title_searched,
            ":rating_searched": rating_searched
        }
    };

    let prom = new Promise(resolve => {
        docClient.query(params, function(err, data) {
            if (err) {
                console.log("Unable to query. Error:", JSON.stringify(err, null, 2));
                resolve("Query failed.");
            } else {
                console.log(data);
                resolve(data.Items);
            }
        });
    }).catch(error => console.log('error', error));

    return prom;
}

const server = async() => { //Creates server and listens. Html file, create, query and delete GETS handled.
    const PORT = process.env.PORT || 3000;

    var app = express();
    app.use(bodyParser.urlencoded({ extended: true }));
    app.use(bodyParser.json());

    let publicPath= path.resolve(__dirname,"public")
    app.use(express.static(publicPath))
    

    app.get('/client', (req, res) => {
        res.sendFile('./movieclient.html', { root: __dirname });
    });
    
    app.get('/create', async(req, res) => {
        var message = await create_db();
        console.log(message);
        res.send({message});
    })

    app.get('/query', async(req, res) => {
        var message = await query_db(parseInt(req.query.year), req.query.title, parseInt(req.query.rating));
        res.send({message});
    })

    app.get('/delete', async(req, res) => {
        var message = await delete_db();
        console.log(message);
        res.send({message});
    })
    
    app.listen(PORT, () => {
       console.log("Example app listening at port: %s", PORT);
    })
}

server();