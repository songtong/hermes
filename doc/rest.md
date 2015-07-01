# 什么是Hermes REST API
Hermes REST API支持消息发布和消息订阅，使用语义与标准SDK一致

## 消息发送REST API
### Topic API
发送消息
```
POST {REST-Server}/topics/:topicName
```
#### Request Header 
Name | Type | Description 
-----|------|------------
`Content-Type`|`string`| application/octet-stream
`X-Hermes-Partition-Key`|`string`| optional, 
`X-Hermes-Priority-Message`|`boolean`| optional, default false
`X-Hermes-Ref-Key`|`string`| optional, default empty
`X-Hermes-Message-Property`|`string`| optional, comma separated, key-value pair.

#### Request Body
Binary only

#### Example(Java)
```java
Client client = ClientBuilder.newClient();
WebTarget webTarget = client.target(RESTServer.HOST);

String topic = "kafka.SimpleTextTopic";

Builder request = webTarget.path("topics/" + topic).request();
request.header("X-Hermes-Priority-Message", "true");
request.header("X-Hermes-Ref-Key", "mykey");
request.header("X-Hermes-Partition-Key", "myPartition");
request.header("X-Hermes-Message-Property", "key1=value1,key2=value2");
String content = "Hello World " + System.currentTimeMillis();
InputStream is = new ByteArrayInputStream(content.getBytes());
Response response = request.post(Entity.entity(is, MediaType.APPLICATION_OCTET_STREAM));
Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
```
#### Response
```json
Status: 200 OK

{
  "topic" : "kafka.SimpleTextTopic",
  "partition" : 1,
  "offset" : 51
}

```

### Subscription API
####注册消息订阅者
```
POST {PORTAL-Server}/subscriptions/
```

#### Request Body
```json
{
  "endpoints":"URL",
  "group":"mygroup",
  "topic":"mytopic"
}
```

#### Example(Java)
```java
Client client = ClientBuilder.newClient();
WebTarget webTarget = client.target(PortalServer._HOST);

String name = "myid";
String topic = "kafka.SimpleTextTopic";
String group = "OneBoxGroup";
String urls = "http://localhost:1357/onebox";

SubscriptionView sub = new SubscriptionView();
sub.setTopic(topic);
sub.setGroup(group);
sub.setEndpoints(urls);
sub.setName(name);

Builder request = webTarget.path("subscriptions/").request();
String json = JSON.toJSONString(sub);
System.out.println("Post: " + json);
Response response = request.post(Entity.entity(json, MediaType.APPLICATION_JSON));
Assert.assertEquals(Response.Status.CREATED.getStatusCode(), response.getStatus());
		
SubscriptionView createdSub = response.readEntity(SubscriptionView.class);
System.out.println("ID :"+createdSub.getId());
```

#### Endpoint 接受消息 Request Header 
Name | Type | Description 
-----|------|------------
`Content-Type`|`string`| application/octet-stream
`X-Hermes-Topic`|`string`| string
`X-Hermes-Ref-Key`|`string`| optional, default empty
`X-Hermes-Message-Property`|`string`| optional, comma separated, key-value pair.

#### 注销消息订阅者
```
DELETE {PORTAL-Server}/subscriptions/:id
```

#### Example
```java
Client client = ClientBuilder.newClient();
WebTarget webTarget = client.target(PortalServer._HOST);

SubscriptionView createdSub = ...;
request = webTarget.path("subscriptions/" + sub.getId()).request();
response = request.delete();
Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
```
