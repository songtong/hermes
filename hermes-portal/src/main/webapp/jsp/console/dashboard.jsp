<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true"%>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld"%>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.dashboard.Context" scope="request" />
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.dashboard.Payload" scope="request" />
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.dashboard.Model" scope="request" />

<a:layout>
	<div>
		<!-- 		<iframe style="width: 100%; height: 600px;"
			src="http://10.2.7.73:5601/#/dashboard/Test-Dashboard?embed&_g=()&_a=(filters:!(),panels:!((col:4,id:Consumer-Received,row:4,size_x:3,size_y:2,type:visualization),(col:7,id:Message-Log-Count,row:1,size_x:6,size_y:5,type:visualization),(col:1,id:Message-QPS,row:1,size_x:6,size_y:3,type:visualization),(col:1,id:NewConsumeTime,row:4,size_x:3,size_y:2,type:visualization)),query:(query_string:(analyze_wildcard:!t,query:'*')),title:Test-Dashboard)">
		</iframe>
 -->
		<iframe
			src="http://10.2.7.73:5601/#/visualize/edit/Message-QPS?embed&_g=()&_a=(filters:!(),linked:!t,query:(query_string:(query:'*')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count)),listeners:(),params:(fontSize:'24'),type:metric))"
			height="600" width="100%"></iframe>
	</div>
</a:layout>