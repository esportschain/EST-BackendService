# -*- coding: utf-8 -*-

__author__ = 'widesky'

# !/usr/bin/env python

import pika
import importlib
import logging
import os

from tornado.options import define, options
import libs.tools as tools



define("vhost", default="esportschain", help="which virtual host")

define("queue_name", default="common_hybrid_queue", help="which queue")
options.parse_command_line()

#try:
# get virtual machine configuration from RabbitMQ
path = os.path.dirname(__file__)
vhost_conf_file = path + '/./conf/queue.yaml'
if not path:
    vhost_conf_file = './conf/queue.yaml'
vhost_conf = tools.get_conf_by_file_name(vhost_conf_file, 'vhost_' + options.vhost)
if not vhost_conf:
    logging.error("not found conf")
    exit(1)

# initialize queue daemon
credentials = pika.PlainCredentials(vhost_conf.get('login'), vhost_conf.get('password'))
connection = pika.BlockingConnection(pika.ConnectionParameters(vhost_conf.get('host'),
                                                               virtual_host=vhost_conf.get('vhost'),
                                                               credentials=credentials))
channel = connection.channel()
queue_name = options.queue_name

# automatic load consumer module
imp_module = importlib.import_module('worker.' + queue_name)
callback = getattr(imp_module, 'callback_' + queue_name)

# Monitor queue consumption
channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue=queue_name)
channel.start_consuming()

#except Exception as e:
#   logging.error('error: %s' % str(e))
