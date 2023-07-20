# -*- coding: utf-8 -*-

from Liquirizia.EventBroker.Configuration import Configuration as ConfigurationBase

__all__ = (
	'Configuration'
)


class Configuration(ConfigurationBase):
	"""
	Configuration Class of Event Broker for AWS
	"""
	def __init__(self, token, secret, region, version=None, timeout=None, max=0):
		self.accessKey = token
		self.accessSecretKey = secret
		self.region = region
		self.version = version
		self.account = None
		self.timeout = timeout
		self.max = 0
		return
