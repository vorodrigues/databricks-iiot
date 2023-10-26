import logging
import threading
import os

def sendMessagesAsyncThread(message_type, device, messages_to_send):
    logging.info(f'TYPE:{message_type} DEVICE:{device} N:{messages_to_send}')
    s = os.popen(f'python3 ./resources/sendMessagesAsync.py --type {message_type} --device {device} --n {messages_to_send}')
    print(s.read())

def iiot_event_generator(n):

  devices = [
    {'type':'turbine', 'id':'WindTurbine-000001', 'n':n},
    {'type':'turbine', 'id':'WindTurbine-000002', 'n':n},
    {'type':'weather', 'id':'WeatherCapture', 'n':n}
  ]

  threads = []

  for device in devices:
    print(f'Sending messages for {device["id"]}')
    t = threading.Thread(target=sendMessagesAsyncThread, args=(device['type'], device['id'], device['n']))
    threads.append(t)
    t.start()
    
  for thread in threads:
    thread.join()