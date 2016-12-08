<html style="font-size: 12px; font-family: sans-serif">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif">
	<div>
		<h3>Broker命令丢弃详情</h3>
		<table border="1" cellpadding="0" cellspacing="0" width="100%">
			<thead>
				<tr>
					<th align="left">主机</th>
					<th align="left">分组</th>
					<th align="left">类型</th>
					<th align="left">数量</th>
				</tr>
			</thead>
			<tbody>
				<#if events??>
				<#list events as event>
				<tr>
					<td align="left"><a href="http://cat.ctripcorp.com/cat/r/e?domain=100003804&date=${event.date?date('yyyy-MM-dd HH:mm:ss')?string('yyyyMMddHH')}&ip=${event.host}&type=Hermes.Command.Drop">${event.host}</a></td>
					<td align="left">${event.group!'UNKNOWN'}</td>
					<td align="left"><a href="http://cat.ctripcorp.com/cat/r/e?domain=100003804&date=${event.date?date('yyyy-MM-dd HH:mm:ss')?string('yyyyMMddHH')}&ip=${event.host}&type=Hermes.Command.Drop">${event.command}</a></td>
					<td align="left">${event.count}</td>
				</tr>
				</#list>
				</#if>
			</tbody>
		</table>
	</div>
	<div>
		<h3>Notes:</h3>
		<ul>
			<li>
				<div>
					<p>
						<b>您可以通过点击主机或者类型链接前往CAT查看丢失详情。
						</b>
					</p>
				</div>
			</li>
		</ul>
	</div>
</body>
</html>