import hmac
import time
import gzip
import zlib
import json
import ujson
import base64
import urllib
import hashlib
import datetime
import aiohttp
import asyncio
import websockets
from base64 import b64decode, b64encode


# constants
client_key_id = "b8038ef5-4312-444c-b537-54c360781ecc"
client_secret = "985f996a2db79f39ab9fd3c35770bdae5a4d8dfe"
REST_PRIVATE_URL = "https://api.xt.com"
REST_PUBLIC_URL = "https://api.xt.com"
WSS_PRIVATE_URL = "wss://xtsocket.xt.com/websocket"
WSS_PUBLIC_URL = "wss://xtsocket.xt.com/websocket"
pair = "QUINT-USDT"



# Bearer
async def bearer():
    async with aiohttp.ClientSession() as client:

        nonce = str(int(time.time() * 1000))
        timestamp = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
        signature = b64encode(hmac.new(b64decode(client_secret), msg = bytes(client_key_id + nonce + timestamp, 'utf-8'), digestmod = hashlib.sha256).digest()).decode('utf-8')
        print(signature)

        data = {
            'client_key_id': client_key_id,
            'timestamp': timestamp,
            'nonce': nonce,
            'signature': signature}
        post_json = json.dumps(data)
        print(post_json)

        headers = {
            "Content-Type": 'application/json'
        }

        url = f"{REST_PRIVATE_URL}/v1/authenticate"
        response = await client.post(url, data=post_json, headers=headers)

        try:
            parsed_response = json.loads(await response.text())
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")
        if response.status != 200:
            raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response}")
        if "error_code" in parsed_response:
            raise IOError(f"{url} API call failed, response: {parsed_response['error_message']}")

        return parsed_response


async def instrument_data(token):
    async with aiohttp.ClientSession() as client:

        headers = {
            "authorization": f"Bearer {token}",
            "Content-Type": 'application/json'
        }

        url = f"{REST_PRIVATE_URL}/v1/account/instrument-data"
        response = await client.get(url, headers=headers)

        try:
            parsed_response = json.loads(await response.text())
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")
        if response.status != 200:
            raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response}")
        if "error_code" in parsed_response:
            raise IOError(f"{url} API call failed, response: {parsed_response['error_message']}")

        return parsed_response


async def wallet_balances():
    async with aiohttp.ClientSession() as client:

        headers = {
            "Content-Type": 'application/x-www-form-urlencoded'
        }

        params = {
            "accesskey": client_key_id,
            "nonce": str(int(time.time() * 1000)),
        }

        payload = urllib.parse.urlencode(dict(sorted(params.items(), key = lambda kv:(kv[0], kv[1]))))

        signature = hmac.new(
            client_secret.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest().upper()

        params["signature"] = signature

        url = f"{REST_PRIVATE_URL}/trade/api/v1/getBalance"
        response = await client.get(url, params=params, headers=headers)

        try:
            parsed_response = json.loads(await response.text())
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")

        if response.status != 200:
            raise IOError(f"Error calling {url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response}")
        if "code" in parsed_response and int(parsed_response["code"]) not in [200, 121, 122]:
            raise IOError(f"{url} API call failed, error message: {parsed_response}")

        return parsed_response


async def trades(token):
    async with aiohttp.ClientSession() as client:

        headers = {
            "authorization": f"Bearer {token}",
            "Content-Type": 'application/json'
        }

        url = f"{REST_PRIVATE_URL}/v1/account/trades"
        response = await client.get(url, headers=headers)

        try:
            parsed_response = json.loads(await response.text())
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")
        if response.status != 200:
            raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response}")
        if "error_code" in parsed_response:
            raise IOError(f"{url} API call failed, response: {parsed_response['error_message']}")

        return parsed_response


async def orders(token):
    async with aiohttp.ClientSession() as client:

        headers = {
            "authorization": f"Bearer {token}",
            "Content-Type": 'application/json'
        }

        url = f"{REST_PRIVATE_URL}/v1/account/order-positions"
        response = await client.get(url, headers=headers)

        try:
            parsed_response = json.loads(await response.text())
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")
        if response.status != 200:
            raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response}")
        if "error_code" in parsed_response:
            raise IOError(f"{url} API call failed, response: {parsed_response['error_message']}")

        return parsed_response


async def transactions(token):
    async with aiohttp.ClientSession() as client:

        headers = {
            "authorization": f"Bearer {token}",
            "Content-Type": 'application/json'
        }

        url = f"{REST_PRIVATE_URL}/v1/account/account-transactions"
        response = await client.get(url, headers=headers)

        try:
            parsed_response = json.loads(await response.text())
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")
        if response.status != 200:
            raise IOError(f"Error fetching data from {url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response}")
        if "error_code" in parsed_response:
            raise IOError(f"{url} API call failed, response: {parsed_response['error_message']}")

        return parsed_response

async def place_order():
    async with aiohttp.ClientSession() as client:

        headers = {
            "Content-Type": 'application/x-www-form-urlencoded'
        }

        params = {
            "accesskey": client_key_id,
            "nonce": str(int(time.time() * 1000)),
            "market": "btc_usdt",
            "price": "19600.01",
            "number": "0.0001",
            "type": 1,
            "entrustType": 0
        }

        payload = urllib.parse.urlencode(dict(sorted(params.items(), key = lambda kv:(kv[0], kv[1]))))

        signature = hmac.new(
            client_secret.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest().upper()

        params["signature"] = signature

        url = f"{REST_PRIVATE_URL}/trade/api/v1/order"
        post_json = json.dumps(params)
        response = await client.post(url, data=params, headers=headers)

        try:
            parsed_response = json.loads(await response.text())
        except Exception as e:
            raise IOError(f"Error parsing data from {url}. Error: {str(e)}")

        if response.status != 200:
            raise IOError(f"Error calling {url}. HTTP status is {response.status}. "
                          f"Message: {parsed_response}")
        if "code" in parsed_response and int(parsed_response["code"]) not in [200, 121, 122]:
            raise IOError(f"{url} API call failed, error message: {parsed_response}")

        return parsed_response



async def start_lmax_private_client(token):
    async def ws_loop():
        headers = {
            "authorization": f"Bearer {token}",
            "Content-Type": 'application/json'
        }
        async with websockets.connect(WSS_PRIVATE_URL, extra_headers=headers) as ws:
            params = {
                "type": "SUBSCRIBE",
                "channels": [
                    "WALLET_BALANCES",
                    "WORKING_ORDERS",
                    "ORDER_POSITIONS"
                ]
            }
            await ws.send(ujson.dumps(params))
            while True:
                print("read:", await ws.recv())

    try:
        await ws_loop()
    except websockets.exceptions.ConnectionClosed as ex:
        print("connection closed", ex)
    except KeyboardInterrupt:
        pass


async def start_xt_public_client():
    async def ws_loop():
        async with websockets.connect(WSS_PUBLIC_URL) as ws:
            params = {
                "channel":  "ex_depth_data",
                "market":   "btc_usdt",
                "event":    "addChannel"
            }
            await ws.send(ujson.dumps(params))
            while True:
                data = await ws.recv()
                # print(data, "\n", type(data))
                message = (ujson.loads(data))
                print(message, "\n\n", type(message))

    try:
        await ws_loop()
    except websockets.exceptions.ConnectionClosed as ex:
        print("connection closed", ex)
    except KeyboardInterrupt:
        pass




async def main():
    # try:
    #     token = await asyncio.wait_for(bearer(), timeout=10.0)
    #     print(json.dumps(token, indent=4))
    # except asyncio.CancelledError:
    #     raise
    # except asyncio.TimeoutError:
    #     print('timeout!')

    # try:
    #     instruments = await asyncio.wait_for(instrument_data(token["token"]), timeout=10.0)
    #     print(json.dumps(instruments, indent=4))
    # except asyncio.CancelledError:
    #     raise
    # except asyncio.TimeoutError:
    #     print('timeout!')
    #
    try:
        wallets = await asyncio.wait_for(wallet_balances(), timeout=10.0)
        print(json.dumps(wallets, indent=4))
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        print('timeout!')

    await asyncio.sleep(1.0)
    #
    # try:
    #     trade_history = await asyncio.wait_for(trades(token["token"]), timeout=10.0)
    #     print(json.dumps(trade_history, indent=4))
    # except asyncio.CancelledError:
    #     raise
    # except asyncio.TimeoutError:
    #     print('timeout!')
    #
    # # await asyncio.sleep(1.0)
    #
    # try:
    #     order_history = await asyncio.wait_for(orders(token["token"]), timeout=10.0)
    #     print(json.dumps(order_history, indent=4))
    # except asyncio.CancelledError:
    #     raise
    # except asyncio.TimeoutError:
    #     print('timeout!')
    #
    # try:
    #     transaction_history = await asyncio.wait_for(transactions(token["token"]), timeout=10.0)
    #     print(json.dumps(transaction_history, indent=4))
    # except asyncio.CancelledError:
    #     raise
    # except asyncio.TimeoutError:
    #     print('timeout!')


    # WSS Private
    # try:
    #     await start_lmax_private_client(token["token"])
    # except asyncio.CancelledError:
    #     raise
    # except asyncio.TimeoutError:
    #     print('timeout!')


    try:
        order = await asyncio.wait_for(place_order(), timeout=10.0)
        print(json.dumps(order, indent=4))
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        print('timeout!')

    # WSS Public
    # try:
    #     await start_xt_public_client()
    # except asyncio.CancelledError:
    #     raise
    # except asyncio.TimeoutError:
    #     print('timeout!')

asyncio.run(main())
