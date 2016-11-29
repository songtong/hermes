<html style="font-size: 12px; font-family: sans-serif">
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif">
	<div>
		<h3>生产详情</h3>
		<table border="1" cellpadding="0" cellspacing="0" width="100%">
			<thead>
				<tr>
					<th align="left">Topic</th>
					<th align="left">各Partition最后生产时间(P代表partition)</th>
					<th align="left">最后生产时间</th>
					<th align="left">生产停止延续时间</th>
					<th align="left">操作</th>
				</tr>
			</thead>
			<tbody>
				<#list events as event>
				<tr>
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/dashboard#/detail/${event.topic}">${event.topic}</a></td>
					<td align="left">
						<ul style="list-style-type:none; padding-left:0px;">
						<#list event.limitsAndStamps?keys as partition>
							<#assign limitAndStamp = event.limitsAndStamps?values[partition_index]>
							<#if limitAndStamp.value.date??>
							<#if latest??>
								<#if latest?long < limitAndStamp.value.date?long>
									<#assign latest = limitAndStamp.value.date>
								</#if>
							<#else>
								<#assign latest = limitAndStamp.value.date>
							</#if>
							</#if>
							<li style="">P${partition}: <#if limitAndStamp.value.date??>${limitAndStamp.value.date?string('yyyy-MM-dd HH:mm:ss')}<#else>尚未使用</#if></li>
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
					<td align="left"><a href="http://hermes.fx.ctripcorp.com/console/dashboard#/detail/${event.topic}">修改阈值</a></td>
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
						<b>告警标示了收集数据时的<font color="blue">瞬时值</font>，如果打开链接发现最新消息ID上升，数据恢复正常，则可忽略告警；如果不是计划内的停止producer，请尽快处理，以免造成业务故障。
						</b>
					</p>
				</div>
			</li>
			<li>
				<div>
					<p>
						<b>点击表格中<font color="blue">Topic</font>可以查看Hermes Dashboad页面，点击右下任意consumer可以看到生产者最新消息生产量。
						</b>
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