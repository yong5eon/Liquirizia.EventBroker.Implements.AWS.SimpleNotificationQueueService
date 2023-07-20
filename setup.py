# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

PKG = 'Liquirizia.EventBroker.Implements.AWS.SimpleNotificationQueueService'
SRC = 'src'
EXCLUDES = []

PKGS = [PKG]
DIRS = {PKG: SRC}
for package in find_packages(SRC, exclude=EXCLUDES):
	PKGS.append('{}.{}'.format(PKG, package))
	DIRS['{}.{}'.format(PKG, package)] = '{}/{}'.format(SRC, package.replace('.', '/'))

setup(
	name='Liquirizia.EventBroker.Implements.AWS.SimpleNotificationQueueService',
	description='EventBroker of Liquirizia for AWS SNS(Simple Notification Service) and SQS(Simple Queue Service)',
	long_description=open('README.md', encoding='utf-8').read(),
	long_description_content_type='text/markdown',
	author='TeamO\'Mine',
	author_email='labs@teamofmine.com',
	version=open('VERSION', encoding='utf-8').read(),
	packages=PKGS,
	package_dir=DIRS,
	include_package_data=False,
	classifiers=[
		'Programming Language :: Python :: 3.8',
	],
	install_requires=[
		'Liquirizia.EventBroker@git+https://github.com/team-of-mine-labs/Liquirizia.EventBroker.git',
		'boto3>=1.24.95'
	],
	url='https://github.com/team-of-mine-labs/Liquirizia',
	python_requires='>=3.8'
)
