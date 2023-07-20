# Liquirizia.EventBroker.Implements.RabbitMQ
AWS 의 SNS(SimpleNotificationService) 과 SQS(SimpleQueueService) 를 사용하는 이벤트 브로커

## 이벤트 컨슈머
```python
from Liquirizia.EventBroker import EventBrokerHelper, Callback
from Liquirizia.EventBroker.Implements.AWS.SimpleNotificationQueueService import (
	Configuration,
	Connection,
	Event,
)

if __name__ == '__main__':

	EventBrokerHelper.Set(
		'Sample',
		Connection,
		Configuration(
			token='${ACCESS_TOKEN}',
			secret='${ACCESS_TOKEN_SECRET}',
			region='${REGION}',
		)
	)

	broker = EventBrokerHelper.Get('Sample')

	# 토픽 선언
	topic = broker.topic()
	topic.declare('TOPIC_SAMPLE')

	# 큐 선언
	queue = broker.queue()
	queue.declare('QUEUE_SAMPLE')
	queue.bind('TOPIC_SAMPLE', 'EVENT_SAMPLE')

	# 이벤트 처리를 위한 콜백 인터페이스 구현
	class SampleCallback(Callback):
		def __call__(self, event: Event):
			try:
				print(event.body)
				event.ack()
			except RuntimeError:
				event.nack()  # if you want requeue message
				# event.reject()  # if you want drop message or move to DLQ or DLE
			return

	# 컨슈머 정의 및 동작
	consumer = broker.consumer(callback=SampleCallback())
	consumer.consume('QUEUE_SAMPLE')
	consumer.run()
```

## 이벤트 퍼블리셔
```python
from Liquirizia.EventBroker import EventBrokerHelper
from Liquirizia.EventBroker.Implements.AWS.SimpleNotificationQueueService import (
	Configuration,
	Connection,
)

from random import randint

if __name__ == '__main__':

	# 이벤트 프로커 설정
	EventBrokerHelper.Set(
		'Sample',
		Connection,
		Configuration(
			token='${ACCESS_TOKEN}',
			secret='${ACCESS_TOKEN_SECRET}',
			region='${REGION}',
		)
	)

	# 토픽에 메세지 퍼블리싱
	EventBrokerHelper.Publish('Sample', 'TOPIC_SAMPLE', event='EVENT_SAMPLE', body=str(randint(0, 1000)), format='text/plain', charset='utf-8')
	# 큐에 메세지 퍼블리싱
	EventBrokerHelper.Send('Sample', 'QUEUE_SAMPLE', event='EVENT_SAMPLE', body=str(randint(0, 1000)), format='text/plain', charset='utf-8')
```

## RPC(Remote Process Call)

### 원격 프로세스 요청
```python
from Liquirizia.EventBroker import EventBrokerHelper
from Liquirizia.EventBroker.Implements.AWS.SimpleNotificationQueueService import (
	Configuration,
	Connection,
)

from random import randint

if __name__ == '__main__':

	# 이벤트 프로커 설정
	EventBrokerHelper.Set(
		'Sample',
		Connection,
		Configuration(
			token='${ACCESS_TOKEN}',
			secret='${ACCESS_TOKEN_SECRET}',
			region='${REGION}',
		)
	)

	# 응답 큐 생성
	EventBrokerHelper.CreateQueue('Sample', 'QUEUE_REPLY')	
	# 큐에 메세지 퍼블리싱
	EventBrokerHelper.Send(
		'Sample', 
		'QUEUE_SAMPLE', 
		event='EVENT_SAMPLE', 
		body=str(randint(0, 1000)), 
		format='text/plain', 
		charset='utf-8',
		headers={
			'X-Broker': 'Sample',
			'X-Broker-Reply': 'QUEUE_REPLY'
		}
	)
	# 응답 요청
	response = EventBrokerHelper.Recv('Sample', 'QUEUE_REPLY')
	# TODO : do something with response
```

### 원격 프로세스 처리
```python
from Liquirizia.EventBroker import EventBrokerHelper, Callback
from Liquirizia.EventBroker.Implements.AWS.SimpleNotificationQeueueService import (
	Configuration,
	Connection,
	Event,
)

if __name__ == '__main__':

	EventBrokerHelper.Set(
		'Sample',
		Connection,
		Configuration(
			token='${ACCESS_TOKEN}',
			secret='${ACCESS_TOKEN_SECRET}',
			region='${REGION}',
		)
	)

	broker = EventBrokerHelper.Get('Sample')

	# 큐 선언
	queue = broker.queue()
	queue.declare('QUEUE_SAMPLE')
	queue.bind('TOPIC_SAMPLE', 'EVENT_SAMPLE')

	# 이벤트 처리를 위한 콜백 인터페이스 구현
	class SampleCallback(Callback):
		def __call__(self, event: Event):
			try:
				# TODO : do something
				EventBrokerHelper.Send(
					event.header('X-Broker'),
					event.header('X-Broker-Reply'),
					event=event.type,
					body=event.body,
					format='plain/text',
					charset='utf-8',
					headers={
						'X-Message-Id': event.id
					}
				)
				event.ack()
			except RuntimeError:
				event.nack()  # if you want requeue message
				# event.reject()  # if you want drop message or move to DLQ or DLE
			return

	# 컨슈머 정의 및 동작
	consumer = broker.consumer(callback=SampleCallback())
	consumer.consume('QUEUE_SAMPLE')
	consumer.run()
```
