<html style="font-size: 12px; font-family: sans-serif">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif">
	<div>
		<h3>Dead Letter详情</h3>
		<table border="1" cellpadding="0" cellspacing="0" width="100%">
			<thead>
				<tr>
					<th align="left">Topic</th>
					<th align="left">Consumer Group</th>
					<th align="left">Dead Letter数量</th>
					<th align="left">操作</th>
				</tr>
			</thead>
			<tbody>
				<#list events as event>
				<tr>
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/topic#/detail/mysql/mysql/${event.topic}">${event.topic}</a></td>
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/dashboard#/deadletter/latest/${event.topic}/${event.consumerGroup}">${event.consumerGroup}</a></td>
					<td align="left">${event.deadLetterCount}</td>
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/consumer#/detail/${event.topic}/${event.consumerGroup}">修改阈值</a></td>
				</tr>
				</#list>
			</tbody>
		</table>
	</div>
	<div>
		<h3>Q&A</h3>
		<ul>
			<li>
				<div>
					<p>
						<b>告警标示了收集数据时的<font color="blue">瞬时值</font>，实际值可能比报警值更大。死信的产生是由于客户端消息处理逻辑异常导致的nack或者应用场景主动nack的次数超过了重发策略规定的次数，因为死信将<font color="red">不再投递</font>，所以请尽快查找原因处理，以免造成业务故障。
						</b>
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>点击表格中<font color="blue">Consumer Group名称</font>可以查看Hermes Dashboad页面, 参看Consumer端消费状况和本地log排查死信原因。
						</b>
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>如果按照提示检查后都不能解决问题，请联系Hermes团队成员，根据团队成员Lync签名找到当周值班同学进行沟通。</b>
					</p>
					<p>
						<b>如遇紧急情况需要联系Hermes团队成员，请拨打<font color="red">Oncall手机：13310027139。</font></b>
					</p>
					<p>
						联系Hermes团队成员时，<font color="red">请提供Topic名称、Consumer名称、App ID等可能相关的信息</font>，非常感谢！
					</p>
				</div>
			</li>
		</ul>
	</div>
</body>
</html>