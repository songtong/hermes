<html style="font-size: 12px; font-family: sans-serif">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif">
	<div>
		<h3>消费详情</h3>
		<table border="1" cellpadding="0" cellspacing="0" width="100%">
			<thead>
				<tr>
					<th align="left">Topic</th>
					<th align="left">Consumer Group</th>
					<th align="left">各个Partition最新消费时间(P代表partition)</th>
					<th align="left">最晚消费时间</th>
					<th align="left">停止消费延续时间</th>
					<th align="left">操作</th>
				</tr>
			</thead>
			<tbody>
				<#list events as event>
				<tr>
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/topic#/detail/mysql/mysql/${event.topic}">${event.topic}</a></td>
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/dashboard#/detail/${event.topic}/${event.consumer}">${event.consumer}</a></td>
					<td align="left">
						<ul style="list-style-type:none;padding-left:0px;">
						<#list event.latestConsumed?values as latestConsume>
							<#if latestConsume?is_first>
							<#assign latest = latestConsume.date>
							<#elseif latest?long < latestConsume.date?long>
							<#assign latest = latestConsume.date>
							</#if>
							<li style="">P${latestConsume.id}: ${latestConsume.date?string('yyyy-MM-dd HH:mm:ss')}</li>
						</#list>
						</ul>
					</td>
					<td align="left">${latest?string('yyyy-MM-dd HH:mm:ss')}</td>
					<#assign seconds = (event.createTime?long - latest?long) / 1000>
					<#assign minutes = (seconds / 60)?int>
					<#assign seconds = seconds % 60>
					<#assign hours = (minutes / 60)?int>
					<#assign minutes = minutes % 60>
					<td align="left"><#if (hours > 0)>${hours}小时</#if><#if (hours > 0 || minutes > 0)>${minutes}分</#if>${seconds}秒</td>
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/dashboard#/detail/${event.topic}/${event.consumer}">修改阈值</a></td>
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
						<b>告警标示了收集数据时的<font color="blue">瞬时值</font>，如果打开链接发现各个partition数据已经在正常消费，则可忽略告警；如果不是计划内的consumer停止造成的报警，请尽快处理，以免造成业务故障。
						</b>
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>点击表格中<font color="blue">Consumer Group名称</font>可以查看Hermes Dashboad页面。
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
						<b>消费至/积压列中<font color="brown">消费纪录停滞</font></b> ：如果部分Partition停滞，请到
						<a href="http://cat.ctripcorp.com/cat/r/e">CAT系统</a>
						确认自己的客户端版本是否为最新。
					</p>
					<p>客户端最新版本请查看<a href="http://conf.ctripcorp.com/pages/viewpage.action?pageId=104759534">此链接</a></p>
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