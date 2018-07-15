var express = require('express');
var path = require('path');
var db = require('pg');

var app = express();

app.use(express.static(path.join(__dirname,'/')));
app.set('view engine', 'ejs');

var dbConnection = "postgres://postgres:postgres@localhost:5432/glonas";

app.get('/searchIds',function(req,res) {
    var dbClient = new db.Client(dbConnection);

    dbClient.connect(function(err){
        if(err)
            throw err;

        var query = "SELECT * from  callcenter.cards WHERE id = ANY ($1)";
        var searchBoxValue = ( typeof req.query.ids != 'undefined' && req.query.ids instanceof Array ) ? req.query.ids : [req.query.ids];



        console.log(searchBoxValue)

        dbClient.query(query , [searchBoxValue], function(err,result){
            if(err)
                throw err;
            else {
                res.render('contacts.ejs' , {contacts: result});
                console.log(result)
                res.end();
            }               
        }); 
    });
});

app.listen(8080,function(){
    console.log('Server started');
});
