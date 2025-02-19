const ws = require("ws");
const fs = require("fs");
const config = require("/js_config").config;
var get_audio_duration = require("get-audio-duration");
import { Trie } from "./trie.js";

// HINT: these particular constants have been choosen that
// it would be sufficient to store 10000*5*5 files that is the
// amount of files in the critical case
let first_layer_count = process.env.FIRST_LAYER_COUNT === undefined
  ? 5
  : parseInt(process.env.FIRST_LAYER_COUNT),
  second_layer_count = process.env.SECOND_LAYER_COUNT === undefined
    ? 5
    : parseInt(process.env.SECOND_LAYER_COUNT);

let maximum_in_directory = process.env.MAXIMUM_IN_DIRECTORY === undefined
  ? 10000
  : parseInt(process.env.MAXIMUM_IN_DIRECTORY);
const wss = new ws.WebSocketServer({ port: config.ws_server.port });
const audio_store_path = config.storage_path;
let storage_state = [];
let storage_tries = [];

function generate_storage_hierarchy() {
  console.log("Generation of the heirarchy has been started");
  let root = config.storage_path;
  for (let t_ = 0; t_ != first_layer_count; ++t_) {
    for (let j_ = 0; j_ != second_layer_count; ++j_) {
      let cur_dir_name = `${root}/${t_ + 1}/${j_ + 1}`;
      fs.access(cur_dir_name, (not_exists) => {
        if (not_exists) {
          fs.mkdir(cur_dir_name, { recursive: true }, (err) => {
            if (err) throw err;
          });
        }
      });
    }
  }
  console.log("All necessary dirs have been created(probably).");
}

function init_storage_state() {
  console.log("Initalization of the hierarchy state has been started");
  let root = config.storage_path;
  for (let t_ = 0; t_ != first_layer_count; ++t_) {
    for (let j_ = 0; j_ != second_layer_count; ++j_) {
      let cur_dir_name = `${root}/${t_ + 1}/${j_ + 1}`;
      storage_state[cur_dir_name] = 0;
      storage_tries[cur_dir_name] = new Trie();
    }
  }
}

function find_out_storage(token) {
  for (let t_ = 0; t_ != first_layer_count; ++t_) {
    for (let j_ = 0; j_ != second_layer_count; ++j_) {
      let cur_dir_name = `${root}/${t_ + 1}/${j_ + 1}`;
      if (storage_tries[cur_dir_name].constains(token)) {
        return cur_dir_name;
      }
    }
  }
}

function delete_case(token) {
  let audio_dir = find_out_storage(token);
  fs.unlink(`${audio_dir}/${token}.wav`, (err) => {
    if (err) console.log(err);
    else {
      console.log(
        `Audio-file with [${token}] uuid has been deleted succesfully.`,
      );
    }
  });
}

async function store_case(token, data) {
  let root = config.storage_path;
  try {
    for (let t_ = 0; t_ != first_layer_count; ++t_) {
      for (let j_ = 0; j_ != second_layer_count; ++j_) {
        let cur_dir_name = `${root}/${t_ + 1}/${j_ + 1}`;
        if (storage_state[cur_dir_name] >= maximum_in_directory) {
          continue;
        }
        fs.writeFile(`${cur_dir_name}/${token}.wav`, data, (err) => {
          if (err) {
            console.log(
              "Error occured with storing audio-file with [${token}] uuid.",
            );
            console.log(err);
          } else {
            storage_tries[cur_dir_name].insert(token);
            storage_state[cur_dir_name]++;
            console.log(
              `Audio-file with uuid [${token}] has been stored succesfully.`,
            );
          }
        });
      }
    }
  } catch (err) {
    console.log(`Error with file openning [${err}].`);
  }
}

async function get_case(ws, token) {
  try {
    let audio_dir = find_out_storage(token);
    const content = fs.readFileSync(`${audio_dir}/${token}.wav`);
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
    let audio_dir = find_out_storage(audio_uuid);
    get_audio_duration.getAudioDurationInSeconds(
      `${audio_dir}/${audio_uuid}.wav`,
    ).then((dur) => {
      ws.send(dur);
    });
  } catch (error) {
    console.error(`File with uuid ${audio_uuid} do not exist`);
    ws.send(0);
  }
}
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

generate_storage_hierarchy();
init_storage_state();
console.log("Start serving!");
