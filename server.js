var AWS = require("aws-sdk");
const { create } = require("domain");
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
        } else {
            console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
        }
    });
}

const get_from_s3 = async() => {
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

    // var allMovies = JSON.parse(fs.readFileSync('moviedata.json', 'utf8'));
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
    dynamodb.describeTable({TableName:"Movies"}, async function(err,result) { // Using describeTable to check if the table exists.
        if(!err){   // If describeTable doesn't throw an error, it means the table exists.
            console.log('Table already exists.');
            delete_db();
        }
        else{       // If describeTable throws an error, the table does not exist, the function proceeds to create and populate the table.
            create_tables();
            const movie_data = await get_from_s3();
            await dynamodb.waitFor("tableExists", {TableName: "Movies"}).promise(); 
            populate_tables(movie_data);
        }
    });
}

const delete_db = () => {
    var params = {
        TableName : "Movies"
    };
    
    dynamodb.deleteTable(params, function(err, data) {
        if (err) {
            console.error("Unable to delete table. Error JSON:", JSON.stringify(err, null, 2));
        } else {
            console.log("Deleted table. Table description JSON:", JSON.stringify(data, null, 2));
        }
    });
}

const query_db = (year, title_searched) => {
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
        } else {
            console.log("Query succeeded.");
            data.Items.forEach(function(item) {
                console.log(" -", item.year + ": " + item.title
                + " ... " + item.info.genres
                + " ... " + item.info.actors[0]);
            });
        }
    });
}

// get_from_s3();
// create_db();
// delete_db();
query_db(2013, "D");

// const get_func = async() => {
//     var movie_data;
//     const get_prom = new Promise(resolve => {
//         movie_data = get_from_s3();
//         resolve(movie_data);
//     })
//     await get_prom;
//     console.log(movie_data);
// }

// get_func();