import { WebSocket } from "ws";
import { config } from "./js_config";
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

async function checkHealth() {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const wsocket = new WebSocket(`ws://${config.ws_server.host}:${config.ws_server.port}`);
      
      return new Promise((resolve, reject) => {
        wsocket.on('open', () => {
          wsocket.send("HEL");
        });

        wsocket.on('message', (data) => {
          const js = JSON.parse(data);
          wsocket.terminate();
          resolve(js.health === 'Healthy' ? 0 : 1);
        });

        wsocket.on('error', (error) => {
          if (attempt === MAX_RETRIES) {
            reject(error);
          }
        });
      });
    } catch (error) {
      if (attempt === MAX_RETRIES) {
        console.error(`Health check failed after ${MAX_RETRIES} attempts:`, error);
        return 1;
      }
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
    }
  }
}

checkHealth()
  .then(code => process.exit(code))
  .catch(() => process.exit(1));
