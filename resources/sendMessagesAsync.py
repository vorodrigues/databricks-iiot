import argparse
import asyncio
import datetime
import json
from random import random, choice, randint
from time import sleep
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message


def getConnStr(deviceId):
    if deviceId == 'WindTurbine-000001':
        conn_str = 'HostName=fe-shared-sa-mfg-iothub.azure-devices.net;DeviceId=WindTurbine-000001;SharedAccessKey=um6+VUtT24ODmEOp/UrbEuwSO8OL2/NZzSPJD3FTUIw='
    elif deviceId == 'WindTurbine-000002':
        conn_str = 'HostName=fe-shared-sa-mfg-iothub.azure-devices.net;DeviceId=WindTurbine-000002;SharedAccessKey=sPJs4/TgyJp3qR+jDnlM4bP8h6QOn8Y86ug9yVljrkk='
    elif deviceId == 'WeatherCapture':
        conn_str = 'HostName=fe-shared-sa-mfg-iothub.azure-devices.net;DeviceId=WeatherCapture;SharedAccessKey=fUw8CxkBlzYnioTrZYQOHmpJe/k+COw0d0XIAn3r+s8='
    else:
        raise Exception('Device ID not found')
    return conn_str


def turbineMessage(deviceId):
    m = {}
    deviceId = 'WindTurbine-'+str(randint(1,500))
    m['deviceId'] = deviceId
    m['timestamp'] = datetime.datetime.now()
    m['rpm'] = 7 * (1 + 0.6 * (-1 + 2 * random()))
    if deviceId == 'WindTurbine-6':
        m['angle'] = 12 * (1 + 0.6 * (-1 + 2 * random()))
    else:
        m['angle'] = 6 * (1 + 0.6 * (-1 + 2 * random()))
    m['power'] = 150 * (1 + 0.3 * (-1 + 2 * random()))
    return Message(json.dumps(m, default=str))
    

def weatherMessage(deviceId):
    m = {}
    m['deviceId'] = deviceId
    m['timestamp'] = datetime.datetime.now()
    m['temperature'] = 27 * (1 + 0.6 * (-1 + 2 * random()))
    m['humidity'] = 64 * (1 + 0.6 * (-1 + 2 * random()))
    m['windspeed'] = 6 * (1 + 0.6 * (-1 + 2 * random()))
    m['winddirection'] = choice(['N','NW','W','SW','S','SE','E','NE'])
    return Message(json.dumps(m, default=str))


def getMessageTemplate(message_type):
    if message_type == 'turbine':
        template = turbineMessage
    elif message_type == 'weather':
        template = weatherMessage
    else:
        raise Exception('Message type not found')
    return template


async def sendMessages():

    # The client object is used to interact with your Azure IoT hub.
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    
    # Connect the client.
    await device_client.connect()
    
    # Prepare message
    async def sendMessage():
      msg = template(deviceId)
      msg.content_encoding = "utf-8"
      msg.content_type = "application/json"
      await device_client.send_message(msg)
    
    print(f'Sending {messages_to_send} messages in parallel...')
    start = datetime.datetime.now()
    
    # send `messages_to_send` messages in parallel
    batchSize = 50
    interval = 1
    sent = 0
    while sent < messages_to_send:
        await asyncio.gather(*[sendMessage() for i in range(0, batchSize)])
        sent = sent + batchSize
        sleep(interval)
    
    total = messages_to_send
    end = datetime.datetime.now()
    duration = (end - start).total_seconds()
    rate = total / duration
    print('===============================================')
    print(f'Start time: {start}')
    print(f'End time: {end}')
    print(f'Duration: {duration} s')
    print(f'Total messages sent: {total}')
    print(f'Rate: {rate} msgs/sec')
    print('===============================================')
    
    # Finally, shut down the client
    await device_client.shutdown()


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", type=str)
    parser.add_argument("--device", type=str)
    parser.add_argument("--n", type=int)
    args = parser.parse_args()

    message_type = args.type
    deviceId = args.device
    messages_to_send = args.n
    
    print(f'Message Type: {message_type}')
    print(f'Device ID: {deviceId}')
    print(f'Message Count: {messages_to_send}')
    
    conn_str = getConnStr(deviceId)
    template = getMessageTemplate(message_type)
    
    asyncio.run(sendMessages())