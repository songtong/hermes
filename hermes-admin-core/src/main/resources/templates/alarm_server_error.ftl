<html style="font-size: 12px; font-family: sans-serif">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif">
	<div>
		<#assign event = events?first>
		<h3><#if event.type == 'BROKER_ERROR'>Broker<#else>Metaserver</#if>错误详情</h3>
		<table border="1" cellpadding="0" cellspacing="0" width="100%">
			<thead>
				<tr>
					<th align="left">主机</th>
					<th align="left">数量</th>
				</tr>
			</thead>
			<tbody>
				<#list events as event>
				<tr>
					<#assign from = event.createTime?long - 300000>
					<td align="left"><a href="http://k5.es.ops.ctripcorp.com/app/kibana#/discover?_g=(refreshInterval:(display:Off,pause:!f,value:0),time:(from:'${from?number_to_datetime?datetime?iso_utc}',mode:absolute,to:'${event.createTime?datetime?iso_utc}'))&_a=(columns:!(_source),index:'hermes-log-*',interval:auto,query:(query_string:(analyze_wildcard:!t,query:'hostname:${event.host}')),sort:!('@timestamp',desc))">${event.host}</a></td>
					<td align="left">${event.errorCount}</td>
				</tr>
				</#list>
			</tbody>
		</table>
	</div>
	<div>
		<h3>错误详情</h3>
		<ul>
			<#list events as event>
			<#if event.errors??>
			<#list event.errors as error>
			<li>
				<div>
					<b>时间: ${error.key?number_to_datetime?datetime?string["yyyy-MM-dd hh:mm:ss"]}  &nbsp;&nbsp; 主机：${event.host}</b>
					<p>
						${error.value}
					</p>
				</div>
			</li>
			</#list>
			</#if>
			</#list>
		</ul>
	</div>
	<div>
		<h3>Q&A</h3>
		<ul>
			<li>
				<div>
					点击host链接跳转到K4查看报警时间段对应主机的报错详细。
				</div>
			</li>
		</ul>
	</div>
</body>
</html>