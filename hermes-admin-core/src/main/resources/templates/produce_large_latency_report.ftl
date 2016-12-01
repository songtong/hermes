<html style="font-size: 12px; font-family: sans-serif">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif">
	<div>
		<h3>发送耗时报告</h3>
		<table border="1" cellpadding="0" cellspacing="0" width="100%">
			<thead>
				<tr>
					<th align="left">Topic</th>
					<th align="left">Broker Group</th>
					<th align="left">非正常平均延时(ms)</th>
					<th align="left">非正常平均延时消息量</th>
					<th align="left">总生产量</th>
					<th align="right">非正常延时消息量比率</th>
					<th align="right">记录时间</th>
				</tr>
			</thead>
			<tbody>
			<#list reports?values as events>
			<#list events as event>
			<tr>
					<td align="left"><a href="http://cat.ctripcorp.com/cat/r/t?domain=All&date=${event.date?date('yyyy-MM-dd HH:mm:ss')?string('yyyyMMddHH')}&ip=All&type=Message.Produce.Elapse.Large">${event.topic}</a></td>
					<td align="left">${event.brokerGroup!'UNKNOWN'}</td>
					<td align="left">${event.latency}</td>
					<td align="left">${event.count}</td>
					<td align="left">${event.countAll}</td>
					<td align="right">${(event.ratio * 100)?string('0.#')}%</td>
					<td align="right">${event.createTime?string('yyyy-MM-dd HH:mm:ss')}</td>
			</tr>
			</#list>
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
						<b>Topic的列出是从上而下按照总比率递减的趋势列出的，生产延时较严重的出现在前面。
						</b>
					</p>
				</div>
			</li>
		</ul>
	</div>
</body>
</html>