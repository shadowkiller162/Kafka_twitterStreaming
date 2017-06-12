
# coding: utf-8
import json
from flask import Flask, request, abort
from linebot import (LineBotApi, WebhookHandler)
from linebot.exceptions import *
from linebot.models import *
import requests
import googleNews
import threading, logging, time, datetime
import _thread
from kafka import KafkaConsumer, KafkaProducer
from linebot import (LineBotApi, WebhookHandler)
import SMS

app = Flask(__name__)

line_bot_api = LineBotApi('yourKey')
handler = WebhookHandler('yourToken')

@app.route("/", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

# use kafka consumer with 3 brokers and 2 partitions, it will stop connect to kafka after 30 sec without get any message
def Consumer(threadName,userId):
    daemon = True
    timeStamp = str(time.mktime(datetime.datetime.now().timetuple()))
    consumer = KafkaConsumer(group_id = timeStamp,
                             bootstrap_servers=['localhost:9092','localhost:9093','localhost:9094'],
                             api_version=(0,9),
                             consumer_timeout_ms=30000
                            )
    consumer.subscribe(['twitterStreaming'])
    for msg in consumer:
        kafakMessage = msg.value.decode("utf-8")
        SMS.sms(kafakMessage)
        line_bot_api.push_message(userId, TextSendMessage(text=kafakMessage))

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    userId = request.json["events"][0]["source"]["userId"]
    # start a new thread to use kafka consumer
    if(event.message.text == "Twitter"):
        _thread.start_new_thread( Consumer, ("kafka", userId) )

if __name__ == "__main__":
    app.run()
