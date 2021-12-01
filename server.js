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

const get_data = () => {
    
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
    })
    await p;
    populate_tables();
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

create_db();

//delete_db();