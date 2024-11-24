import pika
import time
import json
import yaml
import asyncio
import websockets
import os

CFG_PATH = "./config.yml"

connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbit"))

channel = connection.channel()

channel.queue_declare(queue="my_queue", durable=True)
channel.basic_qos(prefetch_count=1)


def get_config():
    try:
        with open(CFG_PATH, "r", encoding="utf-8") as yaml_cfg:
            cfg = yaml.safe_load(yaml_cfg)
        return cfg
    except FileNotFoundError as notexists:
        print(f"Error with config : {notexists}")


async def callback(ch, method, properties, body, ss_conn):
    payload = json.loads(body.decode())

    """
    payload has foll. structure : 
        payload : {
            debt: int,
            accnum: str,
            token: str
        }
    """
    ch.basic_ack(delivery_tag=method.delivery_tag)
    if payload["accnum"] == "":
        return False
    # TTS PART HERE
    #
    #
    #
    tts_out = "output.wav"

    msg = len(payload["token"]).to_bytes(4) + payload["token"].encode()
    try:
        with open("output.wav", mode="r") as wav_data:
            msg += wav_data.read()
            await ss_conn.send(message=msg)
    except FileNotFoundError:
        print("File not found")
    try:
        os.remove(tts_out)
    except FileNotFoundError:
        print("error with file")

    return True


async def get_sound_service_conn(config):
    host = config["sound_service"]["host"]
    port = config["sound_service"]["port"]

    conn = await websockets.connect(uri=f"ws://{host}:{port}")
    return conn


async def main():
    print("Starting consuming!")
    config = get_config()
    if config is None:
        return
    start = time.perf_counter()

    sound_service_connection = await get_sound_service_conn(config)

    for method, properties, body in channel.consume(queue="my_queue"):
        await callback(channel, method, properties, body, sound_service_connection)

    requeued_mess = channel.cancel()
    print(f"Requeued messages : {requeued_mess}")
    channel.close()
    connection.close()
    await sound_service_connection.close()
    print(f"Spent {time.perf_counter() - start}s at all!")


if __name__ == "__main__":
    asyncio.run(main())
