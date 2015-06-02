# 消息的存储格式
## 整体格式
字节范围 | 长度 | 格式 | 说明
------- | ------ | -------- | ----
0-3 | 4 | int | magic number: 1751477619， 即"hems"四个ascii组成的int
4-4 | 1 | byte | 消息格式版本，当前为: 1
5-8 | 4 | int | 消息后续内容的长度
9-12 | 4 | int | 消息头长度
13-16 | 4 | int | 消息体长度
16-15+消息头长度 | 消息头长度 | 消息头 | 消息头
15+消息头长度-14+消息头长度+消息体长度 |消息体长度 | 消息体|消息体
15+消息头长度+消息体长度 | 8 | long | 消息头和消息体的crc32 

## 消息头格式 
字节范围 | 格式 | 说明
------- | ------ | -------- 
16- | string | 消息ref key
- | long | 消息在Producer端生成的时间，即从1970-1-1 00:00:00开始的毫秒数
- | int | 消息剩余的重试次数
- | string | 消息体使用的codec类型，如json, avro
- | map(string->string)| 消息的durable properties
- | map(string->string)| 消息的volatile properties

## 消息体格式 
消息体格式是消息各方事先约定的格式，当前支持json和avro。如果是json类型，可以自行解析。如果是avro格式，可以到Hermes Portal查看和下载相应语言的编解码库包。

## 基本数据类型存储格式 
数据类型 |说明
------- | ------
byte | byte
int | big endian signed int
long | big endian signed long
string | int(后续byte长度，如果为-1则为null)+多个byte(utf8编码的字符串)
map(string->string)| int(如果为-1则为null)+map中存储的keyvalue对数量+多对string(key+value)



