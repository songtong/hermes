<html style="font-size: 12px; font-family: sans-serif">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif">
	<#list reports?keys as reportKey > 
	<#assign topic=reportKey.keys[0]> <#assign consumer=reportKey.keys[1]>
	<div>
		<p>
			Topic: <b style="color: blue"><a href="http://hermes.fx.ctripcorp.com/console/topic#/detail/mysql/mysql/${topic}">${topic}</a></b> ,&emsp;Consumer: <b style="color: blue;"><a
					href="http://hermes.fx.ctripcorp.com/console/dashboard#/detail/${topic}/${consumer}">${consumer}</a></b> ,&emsp;
			<a href="http://hermes.fx.ctripcorp.com/console/consumer#/detail/${topic}/${consumer}">修改阈值</a>
		</p>
		<p>
			Email:&emsp;<b style="color: blue">${reportKey.keys[2]!'NOT SET'}</b>,&emsp;Phone:&emsp;<b style="color: blue">${reportKey.keys[4]!'NOT SET'}</b><br />Email:&emsp;<b style="color: blue">${reportKey.keys[3]!'NOT SET'}</b>,&emsp;Phone:&emsp;<b style="color: blue">${reportKey.keys[5]!'NOT
				SET'}</b>
		</p>
		<table border="1" cellpadding="0" cellspacing="0" width="100%" style="font-size: 12px">
			<thead>
				<tr>
					<th align="left">记录时间</th>
					<th align="left">积压总量</th>
				</tr>
			</thead>
			<tbody>
				<#list reports?values[reportKey_index] as event>
				<tr>
					<td align="left">${event.createTime?string('yyyy-MM-dd HH:mm:ss')}</td>
					<td align="left">${event.totalBacklog}</td>
				</tr>
				</#list>
			</tbody>
		</table>
	</div>
	</#list>
</body>
</html>