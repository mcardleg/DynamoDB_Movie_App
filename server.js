var AWS = require("aws-sdk");
AWS.config.update({
    region: "eu-west-1"
});
var dynamodb = new AWS.DynamoDB();
var fs = require('fs');
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
            ReadCapacityUnits: 10, 
            WriteCapacityUnits: 10
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

const populate_tables = () => {
    console.log("Importing movies into DynamoDB. Please wait.");

    var allMovies = JSON.parse(fs.readFileSync('moviedata.json', 'utf8'));
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
            console.error("Unable to add movie", movie.title, ". Error JSON:", JSON.stringify(err, null, 2));
        } else {
            console.log("PutItem succeeded:", movie.title);
        }
        });
    });
}

const create_db = async() => {
    const p = new Promise(resolve => {
        create_tables();
        resolve();
    }).then(populate_tables());
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

// create_db();
// delete_db();
query_db(2013, "A Field");