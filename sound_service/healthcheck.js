const ws = require("ws");
const fs = require("fs");

const config = require("./config.js").config;

async function main() {


  const wsocket = new ws.WebSocket(`ws://${config.ws_server.host}:${config.ws_server.port}`);

  wsocket.on('error', function handle(error) {
    console.log(`Error is ${error}`);
    process.exit(1);
  });
  wsocket.on('open', function open() {
    wsocket.send("HEL");

  });
  wsocket.on('message', function handler(data) {
    let js = JSON.parse(data);
    let end_value = 1;
    if (js.health == 'Healthy')
      end_value = 0;
    wsocket.terminate();
    process.exit(end_value);
  });
}

main()
