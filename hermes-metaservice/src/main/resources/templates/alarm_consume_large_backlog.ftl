<html>
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body>
	<table border="1" cellpadding="0" cellspacing="0" width="100%">
		<thead>
			<tr>
				<th>Topic</th>
				<th>Consumer</th>
				<th>Backlog</th>
			</tr>
		</thead>
		<tbody>
			<#list events as event>
			<tr>
				<td>${event.topic}</td>
				<td>${event.group}</td>
				<td>${event.totalBacklog}</td>
			</tr>
			</#list>
		</tbody>
	</table>
	
	<hr>
	
	
</body>
</html>