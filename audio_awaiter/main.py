import pika
import pika.exceptions
import time
import json
import yaml
import asyncio
import requests
import os
import sys
import redis
import threading
from typing import List, Dict, Any, Optional, Tuple

CFG_PATH = "/python_config"


class AudioAwaiter:
    def __init__(self):
        self.config = self.construct_helper_cfg()
        if not self.config:
            raise RuntimeError("Failed to load configuration")

        self.redis_clt = self.get_redis_conn()
        if not self.redis_clt:
            raise RuntimeError("Failed to connect to Redis")

        self.connection = None
        self.channel = None
        self.ids_holder = []
        self.dead_consumers_count = 0
        self.timer = None
        self.is_running = True

        if "CONSUMERS_COUNT" not in os.environ:
            raise RuntimeError("CONSUMERS_COUNT environment variable not set")

        self.consumers_count = int(os.environ["CONSUMERS_COUNT"])

    def get_node_token(self, host: str, port: int) -> Optional[str]:
        """
        Utility for taking token from nest-service
        """
        return ""
        # if self.redis_clt.exists("acall_audio_awaiter_token"):
        #    return self.redis_clt.get("acall_audio_awaiter_token")

        # username = self.redis_clt.get("acall_audio_awaiter_username")
        # passw = self.redis_clt.get("acall_audio_awaiter_pass")

        # if not username or not passw:
        #    return None

        # response = requests.get(
        #    f"http://{host}:{port}/login?username={username}&password={passw}"
        # )

        # if response.status_code != 200:
        #    return None

        # payload = response.json()
        # jwt, expire_time_seconds = payload["jwt"], payload["expiresAt"]

        # self.redis_clt.setex("acall_audio_awaiter_token", expire_time_seconds, jwt)
        # print(f"Payload from /login : {payload}")
        # return jwt

    def get_config(self) -> Optional[Dict]:
        try:
            with open(CFG_PATH, "r", encoding="utf-8") as yaml_cfg:
                return yaml.safe_load(yaml_cfg)
        except FileNotFoundError as e:
            print(f"Error with config : {e}")
            return None

    def send_ids(self, ids: List[str]) -> Tuple[Optional[int], Optional[Dict], bool]:
        host = self.config["node"]["host"]
        port = self.config["node"]["port"]

        # Uncomment if you need JWT authentication
        # jwt = self.get_node_token(host, port)
        # if jwt is None:
        #     print("Couldn't get jwt from node!")
        #     return None, None, False

        headers = {
            "Content-Type": "application/json",
            # "Authorization": f"Bearer {jwt}",
        }

        route = self.config["node"]["routes"]["update_audio_readiness"]
        response = requests.post(
            f"http://{host}:{port}/{route}", headers=headers, data=json.dumps(ids)
        )
        return response.status_code, response.json(), True

    def send_wrapper(self, ids: List[str]) -> bool:
        code, text, success = self.send_ids(ids)
        print(f"Payload : {text}, status_code : {code}")
        return success and (code is not None and code <= 201)

    def callback(self, ch, method, properties, body) -> Tuple[bool, bool]:
        """
        Process incoming message
        Returns: (continue_consuming, got_dead_letter)
        """
        ch.basic_ack(delivery_tag=method.delivery_tag)

        payload = json.loads(body.decode())

        if payload["uuid"] != "":
            print(f"Got new uuid to update : [{payload['uuid']}]")
            self.ids_holder.append(payload["uuid"])

            # Check if we reached the data border
            if len(self.ids_holder) >= self.config["data_amount_border"]:
                if self.send_wrapper(self.ids_holder):
                    self.ids_holder.clear()
                return True, False

        else:
            print(
                f"Got dead packet from one of the consumers. Already dead : {self.dead_consumers_count + 1}, "
                f"still alive : {self.consumers_count - self.dead_consumers_count - 1}"
            )

            if self.dead_consumers_count != self.consumers_count - 1:
                return True, True

            # Last consumer - send remaining data and exit
            if self.ids_holder:
                self.send_wrapper(self.ids_holder)
                self.ids_holder.clear()

            print("Got closing packet. Dying...")
            return False, True

        return True, False

    def timeout_callback(self):
        """Timer callback that sends accumulated IDs periodically"""
        if not self.is_running:
            return

        if self.ids_holder:
            print(f"Timeout triggered - sending {len(self.ids_holder)} accumulated IDs")
            if self.send_wrapper(self.ids_holder):
                self.ids_holder.clear()

        # Restart the timer if still running
        if self.is_running:
            self.start_timeout_timer()

    def start_timeout_timer(self, interval_minutes: int = 5):
        """Start the periodic timeout timer"""
        interval_seconds = interval_minutes * 60
        self.timer = threading.Timer(interval_seconds, self.timeout_callback)
        self.timer.daemon = True
        self.timer.start()
        print(f"Started timeout timer for {interval_minutes} minutes")

    async def get_conn_rabbit(self) -> Optional[pika.BlockingConnection]:
        host = self.config["rabbit"]["host"]
        port = self.config["rabbit"]["port"]

        print(f"Trying to connect to amqp://{host}:{port}")
        conn_params = pika.ConnectionParameters(host=host, port=port)

        for _ in range(6):
            try:
                connection = pika.BlockingConnection(conn_params)
                print("Got rabbit connection!!")
                return connection
            except Exception as e:
                print(f"Raised exception while connecting to rabbit : [{e}]")
            await asyncio.sleep(10)
        return None

    def get_redis_conn(self) -> Optional[redis.Redis]:
        try:
            return redis.Redis(
                port=self.config["redis"]["port"],
                host=self.config["redis"]["host"],
                decode_responses=True,
                password=self.config["redis"]["password"],
            )
        except Exception as e:
            print(f"Something wrong with redis connection establishment : {e}")
            return None

    def construct_helper_cfg(self) -> Optional[Dict]:
        config = self.get_config()
        if config is None:
            return None

        helper = {"node": {"routes": {}}, "rabbit": {}, "redis": {}, "stack_name": ""}
        try:
            helper["node"]["port"] = config["services"]["node"]["port"]
            helper["node"]["host"] = config["services"]["node"]["host"]
            helper["node"]["routes"]["update_audio_readiness"] = config["services"][
                "node"
            ]["routes"]["update_audio_readiness"]
            helper["redis"]["port"] = config["services"]["redis"]["port"]
            helper["redis"]["host"] = config["services"]["redis"]["host"]
            helper["redis"]["password"] = config["services"]["redis"]["password"]
            helper["stack_name"] = config["stack_name"]
            helper["rabbit"]["host"] = config["services"]["rabbit"]["host"]
            helper["rabbit"]["port"] = config["services"]["rabbit"]["port"]
            helper["rabbit"]["queue_name"] = config["services"]["rabbit"][
                "ids_queue_name"
            ]
            helper["data_amount_border"] = config["services"]["audio_awaiter"][
                "data_amount_border"
            ]
        except KeyError as e:
            print(f"Error while constructing helper config : {e}")
            return None
        return helper

    async def run(self):
        """Main execution method"""
        print("Starting awaiting!")
        start = time.perf_counter()

        # Connect to RabbitMQ
        self.connection = await self.get_conn_rabbit()
        if self.connection is None:
            print("Got error while establishing connection with rabbit")
            sys.exit(1)

        # Setup channel
        self.channel = self.connection.channel()
        queue_name = self.config["rabbit"]["queue_name"]
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)

        # Start timeout timer (5 minutes by default)
        self.start_timeout_timer(interval_minutes=5)

        try:
            # Start consuming messages
            for method, properties, body in self.channel.consume(queue=queue_name):
                carry_on, got_dead_letter = self.callback(
                    self.channel, method, properties, body
                )

                if got_dead_letter:
                    self.dead_consumers_count += 1

                if not carry_on:
                    break

        except KeyboardInterrupt:
            print("Interrupted by user")
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            self.cleanup()
            print(f"Spent {time.perf_counter() - start}s at all!")

    def cleanup(self):
        """Cleanup resources"""
        self.is_running = False

        # Stop timer
        if self.timer:
            self.timer.cancel()

        # Send any remaining IDs
        if self.ids_holder:
            print(f"Cleaning up - sending {len(self.ids_holder)} remaining IDs")
            self.send_wrapper(self.ids_holder)

        # Close RabbitMQ connections
        if self.channel:
            requeued_mess = self.channel.cancel()
            print(f"Requeued messages : {requeued_mess}")
            self.channel.close()

        if self.connection and not self.connection.is_closed:
            self.connection.close()

        print("Cleanup completed")


async def main():
    try:
        awaiter = AudioAwaiter()
        await awaiter.run()
    except Exception as e:
        print(f"Failed to start AudioAwaiter: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
