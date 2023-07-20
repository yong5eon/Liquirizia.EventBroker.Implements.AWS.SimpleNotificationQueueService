# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Event as EventBase, Error
from Liquirizia.EventBroker.Errors import *

from Liquirizia.EventBroker.Serializer import SerializerHelper
from Liquirizia.EventBroker.Serializer.Errors import (
	NotSupportedError as SerializerNotSupportedError,
	EncodeError as SerializerEncodeError,
	DecodeError as SerializerDecodeError,
)

from Liquirizia.System.Util import GenerateUUID

from botocore.exceptions import ClientError

from time import time

__all__ = (
	'Event'
)


class Event(EventBase):
	"""
	Event of Event Broker for AWS
	"""
	def __init__(self, consumer, message, max=0):
		try:
			# if 'Type' not in message.message_attributes:
			# 	raise InvalidEventError('event is not exist')
			if 'ContentType' not in message.message_attributes:
				raise InvalidEventError('content type is not exist')
			if 'ContentEncoding' not in message.message_attributes:
				raise InvalidEventError('content encoding is not exist')
			self.consumer = consumer
			self.message = message
			self.properties = {
				'ID': self.message.message_attributes['ID']['StringValue'],
				'Type': self.message.message_attributes['Type']['StringValue'] if 'Type' in self.message.message_attributes else None,
				'ContentType': self.message.message_attributes['ContentType']['StringValue'],
				'ContentEncoding': self.message.message_attributes['ContentEncoding']['StringValue'],
				'Persistent': self.message.message_attributes['Persistent']['StringValue'],
				'Body': self.message.body
			}
			if 'Priority' in self.message.message_attributes:
				self.properties['Priority'] = self.message.message_attributes['Priority']['StringValue']
			if 'Timestamp' in self.message.message_attributes:
				self.properties['Timestamp'] = self.message.message_attributes['Timestamp']['StringValue']
			if 'Expiration' in self.message.message_attributes:
				self.properties['Expiration'] = self.message.message_attributes['Expiration']['StringValue']
			if 'Headers' in self.message.message_attributes:
				self.properties['Headers'] = SerializerHelper.Decode(self.message.message_attributes['Headers']['BinaryValue'], 'application/json', 'utf-8')
			self.length = len(self.properties['Body'])
			self.payload = SerializerHelper.Decode(self.properties['Body'].encode(self.properties['ContentEncoding']), self.properties['ContentType'], self.properties['ContentEncoding'])
			self.max = max
		except SerializerNotSupportedError:
			raise NotSupportedTypeError(self.properties['ContentType'], self.properties['ContentEncoding'])
		except SerializerDecodeError:
			raise DecodeError(
				self.properties['Body'],
				self.properties['ContentType'],
				self.properties['ContentEncoding'],
			)
		return

	def __repr__(self):
		return '{} - {} - {}, {}'.format(
			self.properties['Type'],
			self.length,
			self.properties['ContentType'],
			self.properties['ContentEncoding']
		)

	def ack(self):
		try:
			self.message.delete()
		except ClientError as e:
			# TODO : use error factory with boto3 error
			raise Error('Reply Error', error=e)
		return

	def nack(self):
		try:
			self.message.change_visibility(
				VisibilityTimeout=self.max
			)
		except ClientError as e:
			# TODO : use error factory with boto3 error
			raise Error('Reply Error', error=e)
		return

	def reject(self):
		try:
			self.message.change_visibility(
				VisibilityTimeout=0
			)
		except ClientError as e:
			# TODO : use error factory with boto3 error
			raise Error('Reply Error', error=e)
		return

	def headers(self):
		return self.properties['Headers'] if 'Headers' in self.properties else None

	def header(self, key):
		if 'Headers' not in self.properties:
			return None
		if key not in self.properties['Headers']:
			return None
		return self.properties['Headers'][key]

	@property
	def src(self):
		return self.consumer

	@property
	def id(self):
		return self.properties['ID']

	@property
	def type(self):
		return self.properties['Type'] if 'Type' in self.properties else None

	@property
	def body(self):
		return self.payload
