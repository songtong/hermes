<html style="font-size: 12px; font-family: sans-serif">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif">
	<div>
		<h3>消费积压详情</h3>
		<table border="1" cellpadding="0" cellspacing="0" width="100%">
			<thead>
				<tr>
					<th align="left">Topic</th>
					<th align="left">Consumer</th>
					<th align="right">积压总量</th>
					<th align="center">操作</th>
				</tr>
			</thead>
			<tbody>
				<#list events as event>
				<tr>
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/topic#/detail/mysql/mysql/${event.topic}">${event.topic}</a></td>
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/dashboard#/detail/${event.topic}/${event.group}">${event.group}</a></td>
					<td align="right">${event.totalBacklog}</td>
					<td align="center"><a href="http://hermes.fx.ctripcorp.com/console/consumer#/detail/${event.topic}/${event.group}">修改阈值</a></td>
				</tr>
				</#list>
			</tbody>
		</table>
	</div>
	<div>
		<h3>消费积压 Q&A</h3>
		<ul>
			<li>
				<div>
					<p>
						<b>告警标示了收集数据时的<font color="blue">瞬时值</font>，如果打开链接发现数据已恢复正常，则可忽略告警；如果持续收到告警且积压越来越多，请尽快处理，以免造成业务故障。
						</b>
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>点击表格中<font color="blue">Consumer名称</font>可以查看Hermes Dashboad页面。
						</b>
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>Consumer IP 列<font color="brown">看不到自己的消费者IP</font></b> ：请确认消费者是否正确启动，或消费者进程意外退出。<font color="red">请重启消费者并检查进程终止原因</font>。
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>Consumer IP 列<font color="brown">消费者IP时有时无</font></b> ：请确认消费消费机负载是否过高，或消费速度过慢。<font color="red">请尝试降低机器负载，或迁移消费进程至稍空闲的机器</font>。
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>消费至/积压 列中<font color="brown">消费纪录增加，积压也在增加</font></b> ：请确认消费速度是否过慢，是否消息量短时间内暴增。
					</p>
					<p>
						如果持续此现象，<font color="red">请通过增加并发或消费机以提升消费能力</font>；如果短暂此现象，而且积压会回落，<font color="red">请考虑是否有必要提升消费能力</font>。
					</p>
					<p style="font-style: italic;">
						<b>从CAT系统查看消费速度：</b>点击左侧导航的Transaction按钮，搜索框输入自己的App ID，查看Message.Consumed链接对应的统计数据即可。
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>消费至/积压列中<font color="brown">消费纪录停滞</font></b> ：如果部分Partition停滞，请到
						<a href="http://cat.ctripcorp.com/cat/r/e">CAT系统</a>
						确认自己的客户端版本是否为最新。
					</p>
					<p>Java客户端最新版本为0.7.2.5，CAT系统中显示为java-0.7.2.5；.Net Dll最新版本为2.0.7.2，CAT系统中显示为net-0.7.2.6。</p>
					<p>
						<font color="red">快捷解决办法为重启消费者进程，.net一般需要回收IIS。同时请尽快升级到最新版本客户端</font>。
					</p>
					<p style="font-style: italic;">
						<b>从CAT系统查看Hermes客户端版本：</b>点击左侧导航的Event按钮，搜索框输入自己的App ID，找到Hermes.Client.Version，点击查看客户端版本。
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>如果以上情况都不满足，请联系Hermes团队成员，根据团队成员Lync签名找到当周值班同学进行沟通。</b>
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