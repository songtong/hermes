# Java Project
## Producer
1. 确保消息对应的topic已经在Hermes Portal申请通过
1. pom.xml中增加如下依赖

	```xml
	<dependency>
		<groupId>com.ctrip.hermes</groupId>
		<artifactId>hermes-producer</artifactId>
		<version>0.3-SNAPSHOT</version>
	</dependency>
	```
1. 发送消息

	```java    
	Producer p = Producer.getInstance();
	Order order = new Order(...);
	p.message("hotel.order", order.getId(), order).send();
	```

## Consumer
1. 确保消息对应的topic和消费者的groupId已经在Hermes Portal申请通过
1. pom.xml中增加如下依赖

	```xml
	<dependency>
		<groupId>com.ctrip.hermes</groupId>
		<artifactId>hermes-consumer</artifactId>
		<version>0.3-SNAPSHOT</version>
	</dependency>
	```	
1. 接收消息

	```java
	Consumer c = Consumer.getInstance();
	String groupId = "group1";

	c.start("hotel.order", groupId, new BaseMessageListener<Order>(groupId) {
		@Override
		public void onMessage(ConsumerMessage<Order> msg) {
			Order order = msg.getBody();
			// process the order
		}
	});