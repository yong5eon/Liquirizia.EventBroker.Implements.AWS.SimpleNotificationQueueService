# -*- coding: utf-8 -*-

from Liquirizia.EventBroker import EventBrokerHelper
from Liquirizia.EventBroker.Implements.AWS.SimpleNotificiationQueueService import (
	Configuration,
	Connection,
)

from random import randint

if __name__ == '__main__':

	# 이벤트 브로커 설정
	EventBrokerHelper.Set(
		'Sample',
		Connection,
		Configuration(
			token='AKIA3XYP2OGW74DPIFEX',
			secret='PntbOMqwA2AFjEljmaQLPg2H/dhEqdyGYriqvJjR',
			region='ap-northeast-2'
		)
	)

	broker = EventBrokerHelper.Get('Sample')
	topic = broker.topic('TOPIC_SAMPLE')
	queue = broker.queue('QUEUE_SAMPLE')

	topic.publish(
		'EVENT_SAMPLE',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8'
	)
	topic.publish(
		'EVENT_SAMPLE_ERROR',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8'
	)
	queue.send(
		'EVENT_SAMPLE',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8'
	)

	reply = broker.queue()
	reply.declare('QUEUE_REPLY')

	id = topic.publish(
		'EVENT_SAMPLE',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8',
		headers={
			'X-Reply-Broker': 'Sample',
			'X-Reply-Broker-Queue': 'QUEUE_REPLY'
		}
	)
	res = reply.receive()
	print('{} : {} - {}'.format(id, res.body, res.header('X-Reply-Message-Id')))

	id = queue.send(
		'EVENT_SAMPLE',
		str(randint(0, 1000)),
		format='text/plain',
		charset='utf-8',
		headers={
			'X-Reply-Broker': 'Sample',
			'X-Reply-Broker-Queue': 'QUEUE_REPLY'
		}
	)
	res = reply.receive()
	print('{} : {} - {}'.format(id, res.body, res.header('X-Reply-Message-Id')))

	EventBrokerHelper.Publish('Sample', 'TOPIC_SAMPLE', event='EVENT_SAMPLE', body=str(randint(0, 1000)), format='text/plain', charset='utf-8')

	EventBrokerHelper.CreateQueue('Sample', 'QUEUE_REPLY_2')
	id = EventBrokerHelper.Publish('Sample', topic='TOPIC_SAMPLE', event='EVENT_SAMPLE', body=str(randint(0, 1000)), format='text/plain', charset='utf-8', headers={
		'X-Reply-Broker': 'Sample',
		'X-Reply-Broker-Queue': 'QUEUE_REPLY_2'
	})
	res = EventBrokerHelper.Receive('Sample', 'QUEUE_REPLY_2')
	print('{} : {} - {}'.format(id, res.body, res.header('X-Reply-Message-Id')))

	EventBrokerHelper.CreateQueue('Sample', 'QUEUE_REPLY_3')
	id = EventBrokerHelper.Send('Sample', queue='QUEUE_SAMPLE', event='EVENT_SAMPLE', body=str(randint(0, 1000)), format='text/plain', charset='utf-8', headers={
		'X-Reply-Broker': 'Sample',
		'X-Reply-Broker-Queue': 'QUEUE_REPLY_3'
	})
	res = EventBrokerHelper.Receive('Sample', 'QUEUE_REPLY_3')
	print('{} : {} - {}'.format(id, res.body, res.header('X-Reply-Message-Id')))
