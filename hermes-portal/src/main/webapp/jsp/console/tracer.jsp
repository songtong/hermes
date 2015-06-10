<%@ page contentType="text/html; charset=utf-8" isELIgnored="false" trimDirectiveWhitespaces="true" %>
<%@ taglib prefix="a" uri="/WEB-INF/app.tld" %>
<jsp:useBean id="ctx" type="com.ctrip.hermes.portal.console.tracer.Context" scope="request"/>
<jsp:useBean id="payload" type="com.ctrip.hermes.portal.console.tracer.Payload" scope="request"/>
<jsp:useBean id="model" type="com.ctrip.hermes.portal.console.tracer.Model" scope="request"/>

<a:layout>
   View of tracer page under console
</a:layout>