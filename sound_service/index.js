import fs from "fs";

import { access, mkdir, open, readdir, unlink } from "node:fs/promises";
import { basename } from "node:path";
import { config } from "./js_config";
import { WebSocketServer } from "ws";

import { getAudioDurationInSeconds } from "get-audio-duration";

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
const wss = new WebSocketServer({ port: config.ws_server.port });
let storage_state = [];
let storage_tries = [];

let current_firstlayer_number = 1, current_secondlayer_number = 1;

async function generate_storage_hierarchy() {
  console.log("Generation of the heirarchy has been started");
  let root = config.storage_path;
  for (let t_ = 0; t_ != first_layer_count; ++t_) {
    for (let j_ = 0; j_ != second_layer_count; ++j_) {
      let cur_dir_name = `${root}/${t_ + 1}/${j_ + 1}`;
      try {
        await access(cur_dir_name);
      } catch {
        try {
          await mkdir(cur_dir_name, { recursive: true });
        } catch {
          console.error(
            `Cannot create [ ${cur_dir_name} ] directory. Terminating...`,
          );
          process.exit(1);
        }
      }
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
  let root = config.storage_path;
  for (let t_ = 0; t_ != first_layer_count; ++t_) {
    for (let j_ = 0; j_ != second_layer_count; ++j_) {
      let cur_dir_name = `${root}/${t_ + 1}/${j_ + 1}`;
      if (storage_tries[cur_dir_name].contains(token)) {
        return cur_dir_name;
      }
    }
  }
  return "";
}

function find_out_wrapper(token) {
  let start = performance.now();
  let audio_dir = find_out_storage(token);
  let end = performance.now();

  if (audio_dir == "") {
    console.error(
      "Storage does not contain audio with requested uuid : [ ${token} ].",
    );
    return "";
  }
  console.log(
    `In-memory storage for [ ${token} ] uuid is [ ${audio_dir} ], and search took [ ${
      (end - start) / 1000
    }s ] `,
  );
  return audio_dir;
}

async function delete_case(token) {
  const audio_dir = find_out_wrapper(token);

  if (audio_dir == "") {
    return;
  }

  await unlink(`${audio_dir}/${token}.wav`);
  console.log(
    `Audio-file with [ ${token} ] uuid has been deleted succesfully.`,
  );
}

function _shift_storage_ids() {
  if (current_secondlayer_number == second_layer_count) {
    current_firstlayer_number = Math.max(
      (current_firstlayer_number + 1) % (first_layer_count + 1),
      1,
    );
  }
  current_secondlayer_number = Math.max(
    (current_secondlayer_number + 1) % (second_layer_count + 1),
    1,
  );
}

async function store_case(token, data) {
  let root = config.storage_path;
  for (let t_ = 0; t_ != first_layer_count * second_layer_count; ++t_) {
    let cur_dir_name =
      `${root}/${current_firstlayer_number}/${current_secondlayer_number}`;
    if (storage_state[cur_dir_name] >= maximum_in_directory) {
      _shift_storage_ids();
      continue;
    }
    let fd;
    try {
      fd = await open(`${cur_dir_name}/${token}.wav`, "w");
      await fd.writeFile(data);
      storage_tries[cur_dir_name].insert(token);
      storage_state[cur_dir_name]++;
      console.log(
        `Audio-file with uuid [ ${token} ] has been stored succesfully.`,
      );
      _shift_storage_ids();
    } catch {
      console.error(`Error with storing audio-file for [ ${token} ] uuid.`);
      return;
    } finally {
      if (fd !== undefined) {
        await fd.close();
        return;
      }
    }
  }
  console.error(
    `Storage is full! Couldn't store audio-file with [ ${token} ] uuid.`,
  );
}

async function restore_structure() {
  let max_at_second_layer = 0;
  let root = config.storage_path, uuid;
  for (let t_ = 0; t_ != first_layer_count; ++t_) {
    try {
      max_at_second_layer = Math.max(
        (await readdir(`${root}/${t_ + 1}`)).length,
        max_at_second_layer,
      );
    } catch (error) {
      console.error(error);
    }

    for (let k_ = 0; k_ != second_layer_count; ++k_) {
      let cur_dir_name = `${root}/${t_ + 1}/${k_ + 1}`;
      // console.log("Restoring data for [ ${cur_dir_name} ] directory.");
      try {
        const files = await readdir(cur_dir_name);

        if (files.length == 0) {
          console.log(
            `Directory [ ${cur_dir_name} ] is empty. Nothing to be restored.`,
          );
          continue;
        }
        for (const file of files) {
          uuid = basename(file, ".wav");
          storage_tries[cur_dir_name].insert(uuid);
        }
        storage_state[cur_dir_name] = files.length;
        console.log(
          `Found and succesfully restored [ ${files.length} ] audio in [ ${cur_dir_name} ] directory.`,
        );
      } catch (err) {
        console.error(
          `Some error arose : [ ${err} ] while restoring files in [ ${cur_dir_name} ] directory.`,
        );
      }
    }
  }
  second_layer_count = Math.max(second_layer_count, max_at_second_layer);
  try {
    first_layer_count = Math.max(
      first_layer_count,
      (await readdir(root)).length,
    );
  } catch (error) {
    console.error(error);
  }
}

async function get_case(ws, token) {
  const audio_dir = find_out_wrapper(token);

  if (audio_dir == "") {
    ws.send();
    return;
  }
  let fd;
  try {
    fd = await open(`${audio_dir}/${token}.wav`);

    const content = await fd.readFile();
    console.log(
      `There is file ${content.length == 0 ? "but" : "and"} it's ${
        content.length != 0 ? "not" : ""
      } empty`,
    );
    ws.send(content);
  } catch (err) {
    console.log(`Error with file openning [ ${err} ].`);
  } finally {
    await fd.close();
  }
}

async function healthcheck(ws) {
  let response = {
    health: "Healthy",
  };
  console.log(`Got healthcheck and responding ${response}`);
  ws.send(JSON.stringify(response));
}
async function find_file_duration(ws, audio_uuid) {
  try {
    let audio_dir = find_out_storage(audio_uuid);
    getAudioDurationInSeconds(
      `${audio_dir}/${audio_uuid}.wav`,
    ).then((dur) => {
      ws.send(dur);
    });
  } catch (error) {
    console.error(`File with uuid [ ${audio_uuid} ] do not exist`);
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
async function main() {
  init_storage_state();
  if ((await readdir(config.storage_path)).length == 0) {
    generate_storage_hierarchy();
  } else {
    let start = performance.now();
    await restore_structure();
    let end = performance.now();
    console.log(
      `Restore_structure function took [ ${(end - start) / 1000}s ].`,
    );
  }
  console.log(
    `Hierarchy structure is following :\nDirectories in first layer: ${first_layer_count}\nDirectories in second layer: ${second_layer_count}\nCount of files in leaf directories: ${maximum_in_directory}`,
  );
  console.log("Start serving!");
}

main();
