# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Queue as QueueBase, Error
from Liquirizia.EventBroker.Errors import *

from Liquirizia.EventBroker.Serializer import SerializerHelper
from Liquirizia.EventBroker.Serializer.Errors import (
	NotSupportedError as SerializerNotSupportedError,
	EncodeError as SerializerEncodeError,
)

from Liquirizia.System.Util import GenerateUUID
from Liquirizia.Util.List import RemoveDuplicate, Remove

from .Configuration import Configuration
from .Response import Response

from boto3 import resource
from botocore.exceptions import ClientError

from time import time
from queue import Queue as MessageQueue

__all__ = (
	'Queue'
)


class Queue(QueueBase):
	"""
	Queue of Event Broker for RabbitMQ
	"""
	def __init__(self, conf: Configuration, queue: str = None, count: int = 1):
		try:
			self.conf = conf
			self.connection = resource(
				'sqs',
				aws_access_key_id=self.conf.accessKey,
				aws_secret_access_key=self.conf.accessSecretKey,
				region_name=self.conf.region
			)
			self.channel = None
			if queue:
				self.channel = self.connection.get_queue_by_name(
					QueueName=queue,
					QueueOwnerAWSAccountId=str(self.conf.account)
				)
				self.channel.load()
			self.error = None
			self.queue = queue
			self.count = count
			self.responses = MessageQueue()
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SQS', error=e)
			raise ConnectionRefusedError('SQS', error=e)
		return

	def declare(
		self,
		queue: str,
		error: str = None,
		max: int = 1
	):
		try:
			self.channel = self.connection.create_queue(QueueName=queue)
			if error and max:
				try:
					self.error = self.connection.get_queue_by_name(
						QueueName=error,
						QueueOwnerAWSAccountId=str(self.conf.account)
					)
					self.error.load()
				except ClientError as e:
					raise NotFoundError(str(error), error=e)
				statement = {
					"Effect": "Allow",
					"Principal": {
						"Service": "sqs.amazonaws.com"
					},
					"Action": "sqs:sendMessage",
					"Resource": self.error.attributes['QueueArn'],
					"Condition": {
						"ArnEquals": {
							"aws:SourceArn": self.channel.attributes['QueueArn']
						}
					}
				}
				if 'Policy' not in self.error.attributes:
					self.error.attributes['Policy'] = {
						'Version': '2008-10-17',
						'Statement': []
					}
				else:
					self.error.attributes['Policy'] = SerializerHelper.Decode(self.error.attributes['Policy'].encode('utf-8'), 'application/json', 'utf-8')
				if 'Statement' not in self.error.attributes['Policy']:
					self.error.attributes['Policy']['Statement'] = []

				def IsIn(i):
					return i['Action'] == 'sqs:sendMessage' and i['Resource'] == self.error.attributes['QueueArn'] and i['Condition']['ArnEquals']['aws:SourceArn'] == self.channel.attributes['QueueArn']

				self.error.attributes['Policy']['Statement'] = Remove(self.error.attributes['Policy']['Statement'], IsIn)
				self.error.attributes['Policy']['Statement'].append(statement)
				self.error.set_attributes(
					Attributes={
						'Policy': SerializerHelper.Encode(self.error.attributes['Policy'], 'application/json', 'utf-8').decode()
					}
				)
				self.error.reload()
				rp = {'deadLetterTargetArn': self.error.attributes['QueueArn'], 'maxReceiveCount': max}
				self.channel.set_attributes(
					Attributes={
						'RedrivePolicy': SerializerHelper.Encode(rp, 'application/json', 'utf-8').decode()
					}
				)
			self.channel.reload()
			self.queue = queue
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SQS', error=e)
			raise ConnectionRefusedError('SQS', error=e)
		return

	def bind(self, topic: str, event: str):
		try:
			client = resource(
				'sns',
				aws_access_key_id=self.conf.accessKey,
				aws_secret_access_key=self.conf.accessSecretKey,
				region_name=self.conf.region
			)
			tpc = client.Topic(arn='arn:aws:{}:{}:{}:{}'.format('sns', self.conf.region, self.conf.account, topic))
			if not tpc:
				raise NotFoundError(self.queue)
			protocol = 'sqs'
			sub = None
			for s in tpc.subscriptions.all():
				if s.attributes['Protocol'] == protocol and s.attributes['Endpoint'] == self.channel.attributes['QueueArn']:
					sub = s
					break
			if sub:
				filters = SerializerHelper.Decode(sub.attributes['FilterPolicy'].encode('utf-8'), 'application/json', 'utf-8')
				filters['Type'].append(event)
				filters['Type'] = RemoveDuplicate(filters['Type'])
				sub.set_attributes(
					AttributeName='FilterPolicy',
					AttributeValue=SerializerHelper.Encode(filters, 'application/json', 'utf-8').decode()
				)
			else:
				filters = {'Type': []}
				filters['Type'].append(event)
				tpc.subscribe(
					Protocol='sqs',
					Endpoint=self.channel.attributes['QueueArn'],
					Attributes={
						'RawMessageDelivery': "true",
						'FilterPolicy': SerializerHelper.Encode(filters, 'application/json', 'utf-8').decode()
					}
				)
			if 'Policy' not in self.channel.attributes:
				self.channel.attributes['Policy'] = {
					'Version': '2008-10-17',
					'Statement': []
				}
			else:
				self.channel.attributes['Policy'] = SerializerHelper.Decode(self.channel.attributes['Policy'].encode('utf-8'), 'application/json', 'utf-8')
			if 'Statement' not in self.channel.attributes['Policy']:
				self.channel.attributes['Policy']['Statement'] = []

			def IsIn(i):
				return i['Action'] == 'sqs:sendMessage' and i['Resource'] == self.channel.attributes['QueueArn'] and i['Condition']['ArnEquals']['aws:SourceArn'] == tpc.arn

			self.channel.attributes['Policy']['Statement'] = Remove(self.channel.attributes['Policy']['Statement'], IsIn)
			self.channel.attributes['Policy']['Statement'].append(
				{
					"Effect": "Allow",
					"Principal": {
						"Service": "sns.amazonaws.com"
					},
					"Action": "sqs:sendMessage",
					"Resource": self.channel.attributes['QueueArn'],
					"Condition": {
						"ArnEquals": {
							"aws:SourceArn": tpc.arn
						}
					}
				}
			)
			self.channel.set_attributes(
				Attributes={
					'Policy': SerializerHelper.Encode(self.channel.attributes['Policy'], 'application/json', 'utf-8').decode()
				}
			)
			self.channel.reload()
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SQS', error=e)
			raise ConnectionRefusedError('SQS', error=e)
		return

	def send(
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
				'MessageBody': SerializerHelper.Encode(body, format, charset).decode(charset),  # only support string type for Message
				'MessageAttributes': attrs,
			}
			self.channel.send_message(**params)
			return id
		except SerializerNotSupportedError as e:
			raise NotSupportedTypeError(format, charset)
		except SerializerEncodeError as e:
			raise EncodeError(body, format, charset)
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SNS', error=e)
			raise ConnectionRefusedError('SNS', error=e)

	def qos(self, count: int):
		self.count = count
		return

	def recv(self, timeout: int = None):
		try:
			if not self.responses.empty():
				return self.responses.get()
			messages = []
			if timeout:
				messages = self.channel.receive_messages(
					AttributeNames=['All'],
					MessageAttributeNames=['All'],
					MaxNumberOfMessages=self.count,
					WaitTimeSeconds=int(timeout/1000) if timeout else 1
				)
			else:
				while True:
					msgs = self.channel.receive_messages(
						AttributeNames=['All'],
						MessageAttributeNames=['All'],
						MaxNumberOfMessages=self.count,
						WaitTimeSeconds=1
					)
					if msgs:
						messages = msgs
						break
			for message in messages:
				try:
					self.responses.put(Response(message))
				except (
					NotSupportedTypeError,
					EncodeError,
					DecodeError,
					Error,
				) as e:
					# TODO : according to max, decrease change visibility
					message.change_visibility(
						VisibilityTimeout=0
					)
			if not self.responses.empty():
				return self.responses.get()
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SNS', error=e)
			raise ConnectionRefusedError('SNS', error=e)

	def unbind(self, topic: str, event: str):
		try:
			connection = resource(
				'sns',
				aws_access_key_id=self.conf.accessKey,
				aws_secret_access_key=self.conf.accessSecretKey,
				region_name=self.conf.region
			)
			tpc = connection.Topic(arn='arn:aws:{}:{}:{}:{}'.format('sns', self.conf.region, self.conf.account, topic))
			if not tpc:
				raise NotFoundError(self.queue)
			protocol = 'sqs'
			sub = None
			for s in tpc.subscriptions.all():
				if s.attributes['Protocol'] == protocol and s.attributes['Endpoint'] == self.channel.attributes['QueueArn']:
					sub = s
					break
			if not sub:
				return

			filters = SerializerHelper.Decode(sub.attributes['FilterPolicy'].encode('utf-8'), 'application/json', 'utf-8')

			def IsIn(i):
				return i == event
			filters['Type'] = Remove(filters['Type'], IsIn)

			if len(filters['Type']):
				sub.set_attributes(
					AttributeName='FilterPolicy',
					AttributeValue=SerializerHelper.Encode(filters, 'application/json', 'utf-8').decode()
				)
			else:
				sub.delete()
				self.channel.attributes['Policy'] = SerializerHelper.Decode(self.channel.attributes['Policy'].encode('utf-8'), 'application/json', 'utf-8')

				def IsPolicy(i):
					return i['Action'] == 'sqs:sendMessage' and i['Resource'] == self.channel.attributes['QueueArn'] and i['Condition']['ArnEquals']['aws:SourceArn'] == tpc.arn

				self.channel.attributes['Policy']['Statement'] = Remove(self.channel.attributes['Policy']['Statement'], IsPolicy)
				self.channel.set_attributes(
					Attributes={
						'Policy': SerializerHelper.Encode(
							self.channel.attributes['Policy'],
							'application/json',
							'utf-8'
						).decode() if len(self.channel.attributes['Policy']['Statement']) else ''
					}
				)
				self.channel.reload()
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SNS', error=e)
			raise ConnectionRefusedError('SNS', error=e)
		return

	def remove(self):
		try:
			self.channel.delete()
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SNS', error=e)
			raise ConnectionRefusedError('SNS', error=e)
		return
