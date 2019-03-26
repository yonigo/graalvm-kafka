const {promisify} = require('util');
const express = require('express');
const app = express();
const bodyParser = require("body-parser");
const cookieParser = require('cookie-parser');
const request = require('request');
const asyncPost = promisify(request.post);


app.use(cookieParser());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(express.static('public'));
app.use(express.static('public/login'));

app.post('/login', async (req, res) => {
    const session = req.cookies.sid;
    try {
        const response = await asyncPost({url: 'http://detection-service:8080/api/v1/validate', json: true, body: {session: session}});
        if (response.body === 'OK')
            res.send('OK');
        else {
            res.status(401);
            res.send('Bad user');
        }
    }
    catch (e) {
        res.status(500);
        res.send(e);
    }
});

app.listen(8080);