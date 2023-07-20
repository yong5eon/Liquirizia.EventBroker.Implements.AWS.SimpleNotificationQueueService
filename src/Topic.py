# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Topic as TopicBase, Error
from Liquirizia.EventBroker.Errors import *

from Liquirizia.EventBroker.Serializer import SerializerHelper
from Liquirizia.EventBroker.Serializer.Errors import (
	NotSupportedError as SerializerNotSupportedError,
	EncodeError as SerializerEncodeError,
)
from Liquirizia.System.Util import GenerateUUID

from .Configuration import Configuration
from .Event import Event

from boto3 import resource
from botocore.exceptions import ClientError

from time import time

__all__ = (
	'Topic'
)


class Topic(TopicBase):
	"""
	Topic of Event Broker for RabbitMQ
	"""
	def __init__(self, conf: Configuration, topic: str = None):
		try:
			self.conf = conf
			self.connection = resource(
				'sns',
				aws_access_key_id=self.conf.accessKey,
				aws_secret_access_key=self.conf.accessSecretKey,
				region_name=self.conf.region
			)
			self.channel = None
			if topic:
				self.channel = self.connection.Topic(arn='arn:aws:{}:{}:{}:{}'.format('sns', self.conf.region, self.conf.account, topic))
				self.channel.load()
			self.topic = topic
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SNS', error=e)
			raise ConnectionRefusedError('SNS', error=e)

	def declare(self, topic: str, alter: str = None, max: int = None):
		try:
			self.channel = self.connection.create_topic(
				Name=topic,
				Attributes={
					'DisplayName': topic
				}
			)
			if alter and max:
				# TODO : according to error and max, set RedirvePolicy to topic
				pass
			self.topic = topic
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SNS', error=e)
			raise ConnectionRefusedError('SNS', error=e)
		return

	def bind(self, topic: str, event: str):
		raise NotSupportedError('Topic is not support to bind(subscribe) to topic')

	def publish(
		self,
		event,
		body=None,
		format: str = 'application/json',
		charset: str = 'utf-8',
		headers: dict = None,
		priority: int = None,
		expiration: int = None,
		timestamp: int = int(time()),
		persistent: bool = True,
		id: str = GenerateUUID(),
	):
		try:
			attrs = {
				'ID': {
					'DataType': 'String',
					'StringValue': id,
				},
				'Type': {
					'DataType': 'String',
					'StringValue': event,
				},
				'ContentType': {
					'DataType': 'String',
					'StringValue': format,
				},
				'ContentEncoding': {
					'DataType': 'String',
					'StringValue': charset,
				},
				'Persistent': {
					'DataType': 'String',
					'StringValue': 'true' if persistent else 'false',
				},
			}
			if priority:
				attrs['Priority'] = {
					'DataType': 'Number',
					'StringValue': priority,
				}
			if timestamp:
				attrs['Timestamp'] = {
					'DataType': 'String',
					'StringValue': str(timestamp),
				}
			if expiration:
				attrs['Expiration'] = {
					'DataType': 'Number',
					'StringValue': expiration,
				}
			if headers:
				attrs['Headers'] = {
					'DataType': 'Binary',
					'BinaryValue': SerializerHelper.Encode(headers, 'application/json', 'utf-8')
				}
			params = {
				'Message': SerializerHelper.Encode(body, format, charset).decode(charset),  # only support string type for Message
				'MessageAttributes': attrs,
			}
			self.channel.publish(**params)
			return id
		except SerializerNotSupportedError as e:
			raise NotSupportedTypeError(format, charset)
		except SerializerEncodeError as e:
			raise EncodeError(body, format, charset)
		except ClientError as e:
			# TODO : use error factory with boto3 error
			if e.response['Error']['Code'] == 'NotFound':
				raise NotFoundError(self.topic)
			raise Error('Publish to {} Error'.format(self.topic), error=e)

	def unbind(self, topic: str, event: str):
		raise NotSupportedError('Topic is not support to unbind(unsubscribe) to topic')

	def remove(self):
		try:
			# TODO : remove all subscription
			self.channel.delete()
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SNS', error=e)
			raise ConnectionRefusedError('SNS', error=e)
		return
