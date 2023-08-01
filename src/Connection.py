# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Connection as ConnectionBase, Callback, Error
from Liquirizia.EventBroker.Errors import *

from .Configuration import Configuration
from .Topic import Topic
from .Queue import Queue
from .Consumer import Consumer

from boto3 import resource
from botocore.exceptions import ClientError

__all__ = (
	'Connection'
)


class Connection(ConnectionBase):
	"""Connection of Event Broker for AWS Simple Notification Queue Service"""

	def __init__(self, conf: Configuration):
		self.conf = conf
		self.connection = None
		return

	def __del__(self):
		self.close()
		return

	def connect(self):
		try:
			self.conf.account = resource(
				'iam',
				aws_access_key_id=self.conf.accessKey,
				aws_secret_access_key=self.conf.accessSecretKey,
				region_name=self.conf.region
			).CurrentUser().arn.split(':')[4]
		except ClientError as e:
			if e.response['Error']['Code'] == 'AccessDenied':
				raise NotPermittedError('IAM', error=e)
			raise ConnectionRefusedError('IAM', error=e)
		return

	def topic(self, topic: str = None):
		return Topic(self.conf, topic)

	def queue(self, queue: str = None):
		return Queue(self.conf, queue)

	def consumer(self, callback: Callback, count: int = 1):
		return Consumer(self.conf, callback, count=count)

	def close(self):
		pass

