const ws = require("ws");

// const ws_server = ws.WebSocketServer();
const config = require('./config.js').config
const express = require('express');

const app = express();
const server = app.listen(config.ws_server.port);

const wss = new ws.WebSocketServer({ server: server });

console.log('Start serving!')
wss.on('connection', function connection(ws) {
  console.log('Got connection!')
  ws.on('error', console.error);

  ws.on('message', function message(data) {
    // console.log('received: %s', data);
    console.log(data);
    // console.log();
    const token_len = (data.slice(0, 4)).readInt32BE(0);
    const token = data.slice(4, 4 + token_len).toString('ascii');
    console.log(`Token is ${token}`);
    // const t = new ArrayBuffer(data);
    // const length = new Uint32Array(t.slice(0, 4));
    // console.log(length);

    // console.log(t);
    // const jsonn = JSON.parse(t);
    // console.log(jsonn);

  });

  // ws.send('something');
});

app.get('/', (req, res) => {
  res.json({
    msg: 'All fine',
    statusCode: 200
  });
})

// app.listen(config.ws_server.port, () => {
//   console.log(`App is listening on ${config.ws_server.port}`);
// });
