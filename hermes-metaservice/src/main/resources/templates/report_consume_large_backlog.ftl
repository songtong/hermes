<html style="font-size: 12px; font-family: sans-serif">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif">
	<#list reports as entry> <#assign pair=entry.getKey()> <#assign report=entry.getValue()> <#assign topic=pair.key> <#assign consumer=pair.value>
	<div>
		<p>
			Topic: <b style="color: blue"><a href="http://hermes.fx.ctripcorp.com/console/topic#/detail/mysql/mysql/${topic}">${topic}</a></b> ,&emsp;Consumer: <b style="color: blue;"><a
					href="http://hermes.fx.ctripcorp.com/console/dashboard#/detail/${topic}/${consumer}">${consumer}</a></b> ,&emsp;
			<a href="http://hermes.fx.ctripcorp.com/console/consumer#/detail/${topic}/${consumer}">修改阈值</a>
		</p>
		<p>
			Email:&emsp;<b style="color: blue">${report.o1Email!'NOT SET'}</b>,&emsp;Phone:&emsp;<b style="color: blue">${report.o1Phone!'NOT SET'}</b><br />Email:&emsp;<b style="color: blue">${report.o2Email!'NOT SET'}</b>,&emsp;Phone:&emsp;<b style="color: blue">${report.o2Phone!'NOT
				SET'}</b>
		</p>
		<table border="1" cellpadding="0" cellspacing="0" width="100%" style="font-size: 12px">
			<thead>
				<tr>
					<th align="left">纪录时间</th>
					<th align="left">积压总量</th>
				</tr>
			</thead>
			<tbody>
				<#list report.backlogs as backlog>
				<tr>
					<td align="left">${backlog.key}</td>
					<td align="left">${backlog.value}</td>
				</tr>
				</#list>
			</tbody>
		</table>
	</div>
	</#list>
</body>
</html>