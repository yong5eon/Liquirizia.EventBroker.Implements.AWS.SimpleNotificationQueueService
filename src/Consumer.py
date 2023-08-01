# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Consumer as ConsumerBase, Callback, Error
from Liquirizia.EventBroker.Errors import *

from .Event import Event

from .Configuration import Configuration
from boto3 import resource
from botocore.exceptions import ClientError

__all__ = (
	'Consumer'
)


class Consumer(ConsumerBase):
	"""Consumer of Event Broker for AWS Simple Notification Queue Service"""

	INTERVAL = 1000

	def __init__(self, conf: Configuration, callback: Callback, count: int = 1):
		self.conf = conf
		self.count = count
		self.callback = callback
		self.connection = None
		self.channel = None
		self.queue = None
		self.isStop = False
		try:
			self.connection = resource(
				'sqs',
				aws_access_key_id=self.conf.accessKey,
				aws_secret_access_key=self.conf.accessSecretKey,
				region_name=self.conf.region
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('SQS', error=e)
			raise ConnectionRefusedError('SQS', error=e)
		return

	def qos(self, count: int, size: int = 0):
		self.count = count
		return

	def consume(self, queue: str):
		try:
			self.channel = self.connection.get_queue_by_name(
				QueueName=queue,
				QueueOwnerAWSAccountId=str(self.conf.account)
			)
			self.channel.load()
			self.queue = queue
			return self.queue
		except ClientError as e:
			raise NotFoundError(queue, error=e)
		return

	def run(self, interval: int = None):
		try:
			self.isStop = False
			while not self.isStop:
				messages = self.channel.receive_messages(
					AttributeNames=['All'],
					MessageAttributeNames=['All'],
					MaxNumberOfMessages=self.count,
					WaitTimeSeconds=int(interval/1000) if interval else 1
				)
				for message in messages:
					try:
						self.callback(Event(message, self.conf.max))
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
		except ClientError as e:
			raise ConnectionError('SQS', error=e)

	def stop(self):
		self.isStop = True
		return
