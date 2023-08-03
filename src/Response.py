# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import Response as ResponseBase
from Liquirizia.EventBroker.Errors import *

from Liquirizia.Serializer import SerializerHelper
from Liquirizia.Serializer.Errors import (
	NotSupportedError as SerializerNotSupportedError,
	DecodeError as SerializerDecodeError,
)

__all__ = (
	'Response'
)


class Response(ResponseBase):
	"""Response of Event Broker for AWS Simple Notification Queue Service"""

	def __init__(self, message):
		try:
			# if 'Type' not in message.message_attributes:
			# 	raise InvalidEventError('event is not exist')
			if 'ContentType' not in message.message_attributes:
				raise InvalidEventError('content type is not exist')
			if 'ContentEncoding' not in message.message_attributes:
				raise InvalidEventError('content encoding is not exist')
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

	def headers(self):
		return self.properties['Headers'] if 'Headers' in self.properties else None

	def header(self, key):
		if 'Headers' not in self.properties:
			return None
		if key not in self.properties['Headers']:
			return None
		return self.properties['Headers'][key]

	@property
	def id(self):
		return self.properties['ID']

	@property
	def type(self):
		return self.properties['Type'] if 'Type' in self.properties else None

	@property
	def body(self):
		return self.payload
