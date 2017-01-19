<html>
<head>
<meta http-equiv="content-type" content="text/html; charset=UTF-8" />
</head>
<body>
	<table border="0" cellpadding="0" cellspacing="0" width="100%">
		<tr>
			<td><table align="center" cellpadding="0" cellspacing="0" width="750" style="border: 1px solid #cccccc; border-collapse: collapse;">
					<tr><td colspan="3" style="padding: 10px 0px 10px 30px; font-family: Arial, sans-serif; font-size: 25px"><b>日期：</b>${date?date}</td></tr>
					<tr>
						<td rowspan="2" style="padding: 10px 0px 0px 30px; font-family: Arial, sans-serif; font-size: 20px; line-height: 20px;" ><b>概览</b></td>
						<td style="padding: 10px 0px 0px 0px; color: #318AAA; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;"><b>总生产：</b>${total.totalProduce}</td>
						<td style="padding: 10px 30px 0px 0px; color: #318AAA; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;"><b>总消费：</b>${total.totalConsume}</td>
					</tr>
					<tr>
						<td style="padding: 0px 0px 10px 0px; color: #318AAA; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;"><b>有生产Topic数：</b>${total.hasProducedTopicCount}</td>
						<td style="padding: 0px 30px 10px 0px; color: #318AAA; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;"><b>有消费Topic数：</b>${total.hasConsumedTopicCount}</td>
					</tr>
					<tr>
						<td colspan='3' style="padding: 0px 30px 10px 30px">
							<table cellpadding="0" cellspacing="0" width="100%" style="table-layout:fixed">
									<tr>
										<td width="50%" style="vertical-align: text-top; padding-right: 8px;">
											<table cellpadding="0" cellspacing="0" width="100%" style="table-layout:fixed; word-wrap: break-word">
												<tbody style="color: #5D5D5D; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;">
													<tr>
														<td width="70%"><b>生产Top5</b></td>
														<td width="30%" style="text-align:right;"><b>生产量</b></td>
													</tr>
													<#list total.top5Produce as topic>
														<tr>
															<td>${topic.getMiddle()}</td>
															<td style="text-align:right;">${topic.getLast()}</td>
														</tr>
													</#list>
												</tbody>
											</table>
										</td>
										<td width="50%" style="vertical-align: text-top; padding-left: 8px;">
											<table cellpadding="0" cellspacing="0" width="100%" style="table-layout:fixed; word-wrap: break-word">
												<tbody style="color: #5D5D5D; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;">
													<tr>
														<td width="70%"><b>消费Top5</b></td>
														<td width="30%" style="text-align:right;"><b>消费量</b></td>
													</tr>
													<#list total.top5Consume as topic>
														<tr>
															<td>${topic.getMiddle()}</td>
															<td style="text-align:right;">${topic.getLast()}</td>
														</tr>
													</#list>
												</tbody>
											</table>
										</td>
									</tr>
							</table>
						</td>
					</tr>
					<tr>
						<td colspan='3' style="padding: 0px 30px 0px 30px;"><hr></td>
					</tr>
					<tr>
						<td rowspan='2' colspan='3' style="padding: 10px 0px 10px 30px;font-family: Arial, sans-serif; font-size: 20px; line-height: 20px;" ><b>BU分布情况</b></td>
					<tr>
					<tr>
						<td colspan='3' style="padding: 10px 30px 10px 30px;">
							<table cellpadding="0" cellspacing="0" width="100%" style="table-layout:fixed">
									<tr>
										<td width="50%" style="vertical-align: text-top; padding-right: 8px;">
											<table cellpadding="0" cellspacing="0" width="100%" style="table-layout:fixed; word-wrap: break-word">
												<tbody style="color: #5D5D5D; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;">
													<tr>
														<td width="70%"><b>Bu生产Top5</b></td>
														<td width="30%" style="text-align:right;"><b>占比</b></td>
													</tr>
													<#list buProduce as bu> 
														<#if (bu?index >= 5)>
														    <#break>
														 </#if>
														<tr>
															<td>${bu.getKey()}</td>
															<td style="text-align:right;">${bu.getValue()?string.percent}</td>
														</tr>
													</#list>
												</tbody>
											</table>
										</td>
										<td width="50%" style="vertical-align: text-top; padding-left: 8px;">
											<table cellpadding="0" cellspacing="0" width="100%" style="table-layout:fixed; word-wrap: break-word">
												<tbody style="color: #5D5D5D; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;">
													<tr>
														<td width="70%"><b>Bu消费Top5</b></td>
														<td width="30%" style="text-align:right;"><b>占比</b></td>
													</tr>
													<#list buConsume as bu>
														<#if (bu?index >= 5)>
														    <#break>
														 </#if>
														<tr>
															<td>${bu.getKey()}</td>
															<td style="text-align:right;">${bu.getValue()?string.percent}</td>
														</tr>
													</#list>
												</tbody>
											</table>
										</td>
									</tr>
							</table>
						</td>
					</tr>
					<#list buDetails as buDetail>
					<tr>
						<td colspan='3' style="padding: 0px 30px 0px 30px;"><hr></td>
					</tr>
					<tr>
						<td rowspan="2" style="padding: 10px 0px 0px 30px;  font-family: Arial, sans-serif; font-size: 20px; line-height: 20px;"><b>${buDetail.type}</b></td>
						<td style="padding: 10px 0px 0px 0px; color: #318AAA; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;"><b>总生产：</b>${buDetail.totalProduce}</td>
						<td style="padding: 10px 30px 0px 0px; color: #318AAA; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;"><b>总消费：</b>${buDetail.totalConsume}</td>
					</tr>
					<tr>
						<td style="padding: 0px 0px 10px 0px; color: #318AAA; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;"><b>有生产 Topic数：</b>${buDetail.hasProducedTopicCount}</td>
						<td style="padding: 0px 30px 10px 0px; color: #318AAA; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;"><b>有消费Topic数：</b>${buDetail.hasConsumedTopicCount}</td>
					</tr>
					<tr>
						<td colspan='3' style="padding: 0px 30px 10px 30px;">
							<table cellpadding="0" cellspacing="0" width="100%" style="table-layout:fixed">
									<tr>
										<td width="50%" style="vertical-align: text-top; padding-right: 8px;">
											<table cellpadding="0" cellspacing="0" width="100%" style="table-layout:fixed; word-wrap: break-word">
												<tbody style="color: #5D5D5D; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;">
													<tr>
														<td width="70%"><b>生产Top5</b></td>
														<td width="30%" style="text-align:right;"><b>生产量</b></td>
													</tr>
													<#if buDetail.top5Produce??>
														<#list buDetail.top5Produce as topic>
															<tr>
																<td>${topic.getMiddle()}</td>
																<td style="text-align:right;">${topic.getLast()}</td>
															</tr>
														</#list>
													</#if>
												</tbody>
											</table>
										</td>
										<td width="50%" style="vertical-align: text-top; padding-left: 8px;">
											<table cellpadding="0" cellspacing="0" width="100%" style="table-layout:fixed; word-wrap: break-word">
												<tbody style="color: #5D5D5D; font-family: Arial, sans-serif; font-size: 14px; line-height: 20px;">
													<tr>
														<td width="70%"><b>消费Top5</b></td>
														<td width="30%" style="text-align:right;"><b>消费量</b></td>
													</tr>
													<#if buDetail.top5Consume??>
														<#list buDetail.top5Consume as topic>
															<tr>
																<td>${topic.getMiddle()}</td>
																<td style="text-align:right;">${topic.getLast()}</td>
															</tr>
														</#list>
													</#if>
												</tbody>
											</table>
										</td>
									</tr>
							</table>
						</td>
					</tr>
					</#list>
				</table></td>
		</tr>
	</table>
</body>
</html>