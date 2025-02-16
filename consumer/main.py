import pika
from pika import delivery_mode
import pika.exceptions
import time
import json
import yaml
import asyncio
import websockets
from datetime import datetime
import os
import time
import sys

CFG_PATH = "/python_config"


async def get_tts_conn(host, port):
    for _ in range(5):
        try:
            conn = await websockets.connect(
                uri=f"ws://{host}:{port}",
                ping_interval=10,
                ping_timeout=60,
                open_timeout=60,
            )
            print("Got connection to tts!")
            return conn
        except Exception as e:
            print(f"Catched exception while connection to sound_service : {e}")
            await asyncio.sleep(10)

    return None


def get_config():
    try:
        with open(CFG_PATH, "r", encoding="utf-8") as yaml_cfg:
            cfg = yaml.safe_load(yaml_cfg)
        return cfg
    except FileNotFoundError as notexists:
        print(f"Error with config : {notexists}")


async def callback(
    in_ch,
    out_ch_data,
    tts_ch_data,
    method,
    properties,
    body,
    ss_conn,
    very_start_ts,
    stack_name,
):
    message = json.loads(body.decode())
    purpose = message["purpose"]
    payload = message["payload"]
    """
    payload has foll. structure :
    {

        payload : {
            debt: int,
            accnum: str,
            token: str,
            name: str,
            seq: int,
            total: int
        }
    }
    """
    in_ch.basic_ack(delivery_tag=method.delivery_tag)

    if payload["accnum"] == "0" * 12:
        # assumed payload here contains only hostname
        # for tts which is expected to be dead soon
        out_ch_data[0].basic_publish(
            exchange="",
            routing_key=out_ch_data[1],
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        print("Got closing packet. Dying...")
        return False
    # TTS PART HERE
    #
    #
    #
    print("Waiting for free tts host.")

    tts_host, tts_port = None, None
    while True:
        method_frame, _, flat_body = tts_ch_data[0].basic_get(queue=tts_ch_data[1])
        print(method_frame, _, flat_body)
        if method_frame is None:
            print("Still waiting...")
            await asyncio.sleep(1)
            continue
        tts_ch_data[0].basic_ack(delivery_tag=method_frame.delivery_tag)
        body = json.loads(flat_body.decode())
        tts_host = body["host"]
        tts_port = body["port"]
        print(f"Got {body} from tts queue")
        break
    tts_conn = await get_tts_conn(f"{stack_name}_{tts_host}", tts_port)
    if tts_conn is None:
        print("Cannot be attached to tts. Leaving...")
        return
    try:
        print(json.dumps(payload, ensure_ascii=False))
        await tts_conn.send(
            json.dumps(payload, ensure_ascii=False).encode("utf8"), text=True
        )
    except websockets.ConnectionClosedOK as e:
        print(f"Something went wrong : {e}")
        return False
    start = time.perf_counter()
    try:
        async with asyncio.timeout(120):
            wav_data = await tts_conn.recv(decode=False)
    except websockets.ConnectionClosedOK as e:
        print(f"Something went wrong : {e}")
        return False
    print(f"Task consumed {time.perf_counter() - start}s")

    msg = "STR".encode() + len(payload["token"]).to_bytes(1) + payload["token"].encode()
    msg += wav_data
    start = time.perf_counter()
    await ss_conn.send(message=msg, text=False)
    print(f"Sent to sound_service in {time.perf_counter()-start}s")

    start = time.perf_counter()
    out_ch_data[0].basic_publish(
        exchange="",
        routing_key=out_ch_data[1],
        body=json.dumps({"uuid": payload["token"]}),
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
    )
    print(f"Sent to audio_awaiter in {time.perf_counter()-start}s")

    print(
        f"Done {payload['seq']}/{payload['total']} and time passed : {datetime.now() - very_start_ts}"
    )

    print("Releasing tts connection...")
    await tts_conn.close()
    print("TTS connection has released")
    return True


async def get_sound_service_conn(config):
    cfg = config["services"]
    host = cfg["sound_service"]["docker_host"]
    port = cfg["sound_service"]["port"]
    stack_name = config["stack_name"]

    for _ in range(5):
        try:
            conn = await websockets.connect(uri=f"ws://{stack_name}_{host}:{port}")
            print("Got connection to sound_service!")
            return conn
        except Exception as e:
            print(f"Catched exception while connection to sound_service : {e}")
            await asyncio.sleep(10)

    return None


async def get_conn_rabbit(config):
    cfg = config["services"]
    host = cfg["rabbit"]["docker_host"]
    port = cfg["rabbit"]["port"]
    stack_name = config["stack_name"]
    print(f"Trying to connect to amqp://{stack_name}_{host}:{port}")
    conn_params = pika.ConnectionParameters(
        host=f"{stack_name}_rabbit", port=port, blocked_connection_timeout=100
    )

    for _ in range(6):
        try:
            connection = pika.BlockingConnection(conn_params)
            print("Got rabbit connection!!")
            return connection
        except Exception as e:
            print(f"Raised exception while connecting to rabbit : [{e}]")
        await asyncio.sleep(10)
    return None


async def check_postbox_for_death_letter(death_triple_rabb, tts_pair_rabb, stack_name):
    method, properties, body = death_triple_rabb[0].basic_get(
        queue=death_triple_rabb[1]
    )
    if method is None:
        return False
    # FIXME: may be it's redundant
    decoded_body = json.loads(body.decode())
    try:
        print(f"Received mission to assasinate tts on [{decoded_body['host']}] host")
    except KeyError as e:
        print(e)
        return
    tts_host_to_be_shutdown = decoded_body["host"]
    tts_port_to_be_shutdown = decoded_body["port"]
    print("Time to die has come...")
    # FIXME: check if multiple is necessary here
    death_triple_rabb[0].basic_ack(method.delivery_tag)  # , multiple=True)
    while True:
        method_frame, _, flat_body = tts_pair_rabb[0].basic_get(queue=tts_pair_rabb[1])
        print(method_frame, _, flat_body)
        if method_frame is None:
            print("Still waiting...")
            await asyncio.sleep(1)
            continue
        temp_body = json.loads(flat_body)
        print(
            f'Making comparison between {tts_host_to_be_shutdown} and {stack_name}_{temp_body["host"]}'
        )
        if tts_host_to_be_shutdown != f'{stack_name}_{temp_body["host"]}':
            print(f'Got inocent [{temp_body["host"]}]. Wait a little...')
            tts_pair_rabb[0].basic_nack(method_frame.delivery_tag)
        tts_pair_rabb[0].bask_ack(method_frame.delivery_tag)
        print("Got him at last! Chasing...")

        break
    tts_conn = await get_tts_conn(tts_host_to_be_shutdown, tts_port_to_be_shutdown)
    if tts_conn is None:
        print("Cannot be attached to tts. Leaving...")
        return False
    try:
        print(f"Assasins was sent to [{tts_host_to_be_shutdown}]")
        await tts_conn.send(
            json.dumps({"accnum": "0" * 12}, ensure_ascii=False).encode("utf8"),
            text=True,
        )
    except websockets.ConnectionClosedOK as e:
        print(f"Something went wrong : {e}")
        return False
    await tts_conn.close()
    death_triple_rabb[0].basic_publish(
        exchange="",
        routing_key=death_triple_rabb[2],
        body=json.dumps(
            {tts_host_to_be_shutdown: "Has been notifyed about the death sentence"}
        ),
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
    )
    return True


async def main():
    print("Starting consuming!")
    config = get_config()
    if config is None:
        return
    start = time.perf_counter()

    sound_service_connection = await get_sound_service_conn(config)
    if sound_service_connection is None:
        print("Cannot connect to sound service. Comitting suicide...")
        sys.exit(1)

    # tts_connection = await get_tts_conn(config)
    # if tts_connection is None:
    #     print("Cannot connect to tts_service. Comitting suicide...")
    #     sys.exit(1)

    connection = await get_conn_rabbit(config)
    if connection is None:
        await sound_service_connection.close()
        # await tts_connection.close()
        print("Got error while establising connection with rabbit")
        sys.exit(1)

    in_channel = connection.channel()
    out_channel = connection.channel()
    tts_channel = connection.channel()
    death_channel = connection.channel()

    in_queue_name = config["services"]["rabbit"]["data_queue_name"]
    out_queue_name = config["services"]["rabbit"]["ids_queue_name"]
    tts_queue_name = config["services"]["rabbit"]["tts_queue_name"]
    death_req_queue_name = config["services"]["rabbit"]["death_req_queue_name"]
    death_ack_queue_name = config["services"]["rabbit"]["death_ack_queue_name"]

    in_channel.queue_declare(queue=in_queue_name, durable=True)
    in_channel.basic_qos(prefetch_count=1)

    out_channel.queue_declare(queue=out_queue_name, durable=True)

    tts_channel.queue_declare(queue=tts_queue_name, durable=True)

    death_channel.queue_declare(queue=death_ack_queue_name, durable=True)
    death_channel.queue_declare(queue=death_req_queue_name, durable=True)
    death_channel.basic_qos(prefetch_count=1)

    # _, _, body = in_channel.basic_get(queue=in_queue_name, auto_ack=True)

    # total_amount_of_rows = json.loads(body)["count"]
    # print(f"Total count of rows to handle is {total_amount_of_rows}")
    very_start = datetime.now()
    for method, properties, body in in_channel.consume(queue=in_queue_name):
        if method is None or body is None:
            # TODO: it wont hurt to call check_postbox also here
            continue
        carry_on = await callback(
            in_channel,
            (out_channel, out_queue_name),
            (tts_channel, tts_queue_name),
            method,
            properties,
            body,
            sound_service_connection,
            very_start,
            config["stack_name"],
        )
        if not carry_on:
            break
        if check_postbox_for_death_letter(
            (death_channel, death_req_queue_name, death_ack_queue_name),
            (tts_channel, tts_queue_name),
            config["stack_name"],
        ):
            break

    requeued_mess = in_channel.cancel()
    death_channel.cancel()
    print(f"Requeued messages : {requeued_mess}")
    in_channel.close()
    out_channel.close()
    tts_channel.close()
    death_channel.close()
    connection.close()
    await sound_service_connection.close()
    print(f"Spent {time.perf_counter() - start}s at all!")


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0)
