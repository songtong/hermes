<html>
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body>
	<h4>消费积压详情</h4>
	<hr>
	<table border="1" cellpadding="0" cellspacing="0" width="100%">
		<thead>
			<tr>
				<th>Topic</th>
				<th>Consumer</th>
				<th>积压总量</th>
			</tr>
		</thead>
		<tbody>
			<#list events as event>
			<tr>
				<td>${event.topic}</td>
				<td><a href="http://hermes.fx.ctripcorp.com/console/dashboard#/detail/${event.topic}/${event.group}">${event.group}</a></td>
				<td>${event.totalBacklog}</td>
			</tr>
			</#list>
		</tbody>
	</table>
	<br>
	<br>
	<h4>消费积压 Q&A</h4>
	<hr>
	<ul>
		<li>
			<b>点击表格中链接查看Hermes Dashboad页面</b>
			<ul>
				<li>
					<p>
						<b>“Consumer IP”列看不到自己的消费者IP</b> ：请确认消费者是否正确启动，或消费者进程意外退出。<font color="red">请重启消费者并检查进程终止原因</font>。
					</p>
				</li>
				<li>
					<p>
						<b>“Consumer IP”列消费者IP时有时无</b> ：请确认消费消费机负载是否过高，或消费速度过慢。<font color="red">请尝试降低机器负载，或迁移消费进程至稍空闲的机器</font>。
					</p>
				</li>
				<li>
					<p>
						<b>“消费至/积压”列中消费纪录增加，积压也在增加</b> ：请确认消费速度是否过慢，是否消息量短时间内暴增。如果持续此现象，<font color="red">请通过增加并发或消费机以提升消费能力</font>；如果短暂此现象，而且积压会回落，<font color="red">请考虑是否有必要提升消费能力</font>。
					</p>
				</li>
				<li>
					<b>“消费至/积压”列中消费纪录停滞</b> ：如果部分Partition停滞，请到
					<a href="http://cat.ctripcorp.com/cat/r/e">CAT系统</a>
					确认自己的客户端版本是否为最新。Java客户端最新版本为0.7.2.5，CAT系统中显示为java-0.7.2.5；.Net Dll最新版本为2.0.7.2，CAT系统中显示为net-0.7.2.6。<font color="red">快捷解决办法为重启消费者进程，.net一般需要回收IIS。同时请尽快升级到最新版本客户端</font>。
					<p style="font-size: 10; font-style: italic;">
						<b>从CAT系统查看Hermes客户端版本：</b>点击左侧导航的Event按钮，找到Hermes.Client.Version，点击查看客户端版本
					</p>
				</li>
			</ul>
		</li>
		<li>
			<p>
				<b>如果以上情况都不满足，请联系Hermes团队成员，根据团队成员Lync签名找到当周值班同学进行沟通。</b>
			</p>
			<p>
				联系Hermes团队成员时，<font color="red">请提供Topic名称、Consumer名称、App ID等可能相关的信息</font>，非常感谢！
			</p>
		</li>
	</ul>
</body>
</html>