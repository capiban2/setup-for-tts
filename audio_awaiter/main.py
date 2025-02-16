import pika
import pika.exceptions
import time
import json
import yaml
import asyncio
import requests

import os
import time
import sys
import redis

CFG_PATH = "/python_config"


def get_node_token(host, port, redis_conn):
    """
    Utility for taking token from nest-service
    """
    if redis_conn.exists("acall_audio_awaiter_token"):
        return redis_conn.get("acall_audio_awaiter_token")
    username = redis_conn.get("acall_audio_awaiter_username")
    passw = redis_conn.get("acall_audio_awaiter_pass")
    response = requests.get(
        f"http://{host}:{port}/login?username={username}&password={passw}"
    )
    if response.status_code != 200:
        return None

    payload = response.json()

    jwt, expire_time_seconds = payload["jwt"], payload["expiresAt"]

    redis_conn.setex("acall_audio_awaiter_token", expire_time_seconds, jwt)

    print(f"Payload from /login : {payload}")
    return jwt


def get_config():
    try:
        with open(CFG_PATH, "r", encoding="utf-8") as yaml_cfg:
            cfg = yaml.safe_load(yaml_cfg)
        return cfg
    except FileNotFoundError as notexists:
        print(f"Error with config : {notexists}")


def send_ids(ids, cfg, rdis_clt):
    host = cfg["host"]
    port = cfg["port"]
    jwt = get_node_token(host, port, rdis_clt)
    if jwt is None:
        return None, None, False
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt}",
    }

    route = cfg["routes"]["update_audio_readiness"]

    response = requests.post(
        f"http://{host}:{port}/{route}", headers=headers, data=json.dumps(ids)
    )
    return response.status_code, response.json(), True


def send_wrapper(ids, redis_clt, cfg):
    code, text, __rslt = send_ids(ids, cfg["node"], redis_clt)
    print(f"Response : {text}")
    if not __rslt or (code is not None and code > 201):
        return False
    return True


def callback(ch, method, properties, body, cfg, ids_holder: list, redis_clt):
    data_count_border = cfg["data_amount_border"]

    ch.basic_ack(delivery_tag=method.delivery_tag)

    payload = json.loads(body.decode())
    print(f"Got new uuid to update : [{payload['uuid']}]")
    if payload["uuid"] == "":
        send_wrapper(ids_holder, redis_clt, cfg)
        print("Got closing packet. Dying...")
        return False
    ids_holder.append(payload["uuid"])
    if len(ids_holder) < data_count_border:
        return True
    rslt = send_wrapper(ids_holder, redis_clt, cfg)
    ids_holder = []
    return rslt


async def get_conn_rabbit(cfg):
    host = cfg["rabbit"]["host"]
    port = cfg["rabbit"]["port"]

    print(f"Trying to connect to amqp://{host}:{port}")
    conn_params = pika.ConnectionParameters(host=f"{host}", port=port)

    for _ in range(6):
        try:
            connection = pika.BlockingConnection(conn_params)
            print("Got rabbit connection!!")
            return connection
        except Exception as e:
            print(f"Raised exception while connecting to rabbit : [{e}]")
        await asyncio.sleep(10)
    return None


def get_redis_conn(cfg):
    try:
        redis_conn = redis.Redis(
            port=cfg["redis"]["port"],
            host=cfg["redis"]["host"],
            decode_responses=True,
            password=cfg["redis"]["password"],
        )
        return redis_conn
    except Exception as e:
        print(f"Something wrong with redis connection establishment : {e}")
    return None


def construct_helper_cfg():
    config = get_config()
    if config is None:
        return None

    helper = {"node": {"routes": {}}, "rabbit": {}, "redis": {}, "stack_name": ""}
    try:
        helper["node"]["port"] = config["services"]["node"]["port"]
        helper["node"]["host"] = config["services"]["node"]["host"]
        helper["node"]["routes"]["update_audio_readiness"] = config["services"]["node"][
            "routes"
        ]["update_audio_readiness"]
        helper["redis"]["port"] = config["services"]["redis"]["port"]
        helper["redis"]["host"] = config["services"]["redis"]["host"]
        helper["redis"]["password"] = config["services"]["redis"]["password"]
        helper["stack_name"] = config["stack_name"]
        helper["rabbit"]["host"] = config["services"]["rabbit"]["host"]
        helper["rabbit"]["port"] = config["services"]["rabbit"]["port"]
        helper["rabbit"]["queue_name"] = config["services"]["rabbit"]["ids_queue_name"]
        helper["data_amount_border"] = config["services"]["audio_awaiter"][
            "data_amount_border"
        ]
    except KeyError as e:
        print(f"Error while constructing helper config : {e}")
        return None
    return helper


async def main():
    print("Starting awaiting!")
    config = construct_helper_cfg()
    if config is None:
        return
    start = time.perf_counter()

    connection = await get_conn_rabbit(config)
    if connection is None:
        print("Got error while establising connection with rabbit")
        sys.exit(1)

    redis_clt = get_redis_conn(config)
    if redis_clt is None:
        print("Got error while establising connection with redis")
        sys.exit(1)
    channel = connection.channel()
    queue_name = config["rabbit"]["queue_name"]
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)

    ids_holder = []

    for method, properties, body in channel.consume(queue=queue_name):
        carry_on = callback(
            channel, method, properties, body, config, ids_holder, redis_clt
        )
        if not carry_on:
            break

    requeued_mess = channel.cancel()
    print(f"Requeued messages : {requeued_mess}")
    channel.close()
    connection.close()
    print(f"Spent {time.perf_counter() - start}s at all!")


if __name__ == "__main__":
    asyncio.run(main())
