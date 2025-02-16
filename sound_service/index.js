const ws = require("ws");
const fs = require("fs");
const config = require("/js_config").config;
var get_audio_duration = require("get-audio-duration");

const wss = new ws.WebSocketServer({ port: config.ws_server.port });
const audio_store_path = config.storage_path;

function delete_case(token) {
  fs.unlink(`${audio_store_path}/${token}.wav`, (err) => {
    if (err) console.log(err);
  });
}

async function store_case(token, data) {
  try {
    fs.writeFile(`${audio_store_path}/${token}.wav`, data, (err) => {
      if (err) {
        console.log("Error occured");
        console.log(err);
      } else {
        console.log(`Audio with  token [${token}] was stored succesfully`);
      }
    });
  } catch (err) {
    console.log(`Error with file openning [${err}].`);
  }
}

async function get_case(ws, token) {
  try {
    const content = fs.readFileSync(`${audio_store_path}/${token}.wav`);
    console.log(
      `There is file ${content.length == 0 ? "but" : "and"} its ${content.length != 0 ? "not" : ""
      } empty`,
    );
    ws.send(content);
  } catch (err) {
    console.log(`Error with file openning [${err}].`);
  }
}

async function healthcheck(ws) {
  let response = {
    health: "Healthy",
  };
  ws.send(JSON.stringify(response));
}
async function find_file_duration(ws, audio_uuid) {
  try {
    get_audio_duration.getAudioDurationInSeconds(
      `${audio_store_path}/${audio_uuid}.wav`,
    ).then((dur) => {
      ws.send(dur);
    });
  } catch (error) {
    console.error(`File with uuid ${audio_uuid} do not exist`);
    ws.send(0);
  }
}
console.log("Start serving!");
wss.on("connection", function connection(ws) {
  console.log("Got connection!");
  ws.on("error", console.error);

  ws.on("message", async function message(data) {
    // console.log(data);
    const method = data.slice(0, 3).toString("ascii");
    let token_len = null, token = null;

    if (method != "HEL") {
      console.log(`Requested method is ${method}`);

      token_len = (data.slice(3)).readInt8(0);

      token = data.slice(4, 4 + token_len).toString("ascii");
      console.log(token);
    }

    // console.log(token_len);

    switch (method) {
      case "GET":
        await get_case(ws, token);
        break;
      case "STR":
        const wav_data = data.slice(4 + token_len);
        await store_case(token, wav_data);
        break;
      case "DEL":
        delete_case(token);
        break;
      case "HEL":
        healthcheck(ws);
        break;
      case "LEN":
        find_file_duration(ws, token);
        break;
      default:
    }
  });
});
