function get(url, success) {
	$.ajax({
		type : 'GET',
		url : url,
		crossDomain : true,
		dataType : 'json',
		success : success,
		error : function(responseData, textStatus, errorThrown) {
			alert(errorThrown);
		}
	});
}

function Trace() {
	this.commonEvents = [];
	this.consumerEvents = {};
	this.consumerStartEvents = {};
	this.consumerEndEvents = {};

	this.textShown = {};
	this.textShown["Message.Born"] = "BORN";
	this.textShown["Message.Received"] = "RCV";
	this.textShown["Message.Saved"] = "SAVE";
	this.textShown["Message.Delivered"] = "DELIVER";
	this.textShown["Message.BizProcessStart"] = "START";
	this.textShown["Message.BizProcessEnd"] = "END";

	this.series = function() {
		var series = [];
		var groupCount = Object.keys(this.consumerEvents).length
		var newEmptyData = function() {
			var data = [];
			for (var i = 0; i <= groupCount; i++) {
				data[i] = [];
			}
			return data;
		}.bind(this);

		var data = newEmptyData();
		data[0] = [ this.commonEvents[1].x, this.commonEvents[2].x ];
		series.push({
			name : 'RCV',
			data : data
		});
		data = newEmptyData();
		data[0] = [ 0, this.commonEvents[1].x ];
		series.push({
			name : 'BORN',
			data : data
		});

		var groupIds = Object.keys(this.consumerEvents);
		for (var i = 0; i < this.maxConsumerEventCount - 1; i++) {
			data = newEmptyData();
			for (var j = 0; j < groupCount; j++) {
				var groupId = groupIds[j];
				var groupEvents = this.consumerEvents[groupId];
				data[j + 1] = [
						groupEvents[this.maxConsumerEventCount - i - 2].x,
						groupEvents[this.maxConsumerEventCount - i - 1].x ];
			}
			series.push({
				name : 's',
				data : data
			});
		}

		data = newEmptyData();
		for (var i = 0; i < groupCount; i++) {
			var groupId = groupIds[i];
			var groupEvents = this.consumerEvents[groupId];
			data[i + 1] = [ this.commonEvents[2].x, groupEvents[0].x ];
		}
		series.push({
			name : 's',
			data : data
		});
		console.log(series);
		return series;
	};

	this.plotLines = function() {
		var plotLines = [];

		var minStartEvent = null;
		var minEndEvent = null;
		for (groupId in this.consumerEvents) {
			if (!minStartEvent
					|| this.consumerEvents[groupId][0].x < minStartEvent.x) {
				minStartEvent = this.consumerEvents[groupId][0];
			}
			if (!minEndEvent
					|| this.consumerEvents[groupId][1].x < minEndEvent.x) {
				minEndEvent = this.consumerEvents[groupId][1];
			}
		}

		console.log(this.commonEvents);

		var plots = [ {
			text : "BORN",
			value : 0
		}, {
			text : "RCV",
			value : this.commonEvents[1].x
		}, {
			text : "SAVE",
			value : this.commonEvents[2].x
		}, {
			text : "DELIVER",
			value : this.commonEvents[2].x + this.commonEvents[2].width
		} ];
		if (minStartEvent) {
			plots.push({
				text : "START",
				value : minStartEvent.x + minStartEvent.width
			});
			plots.push({
				text : "END",
				value : minEndEvent.x + minEndEvent.width
			});
		}

		var lastPlotLine = null;
		plots.forEach(function(plot) {
			var yaxis = 0;
			if (lastPlotLine != null) {
				if (plot.value - lastPlotLine.value < 20) {
					yaxis = 50;
				}
			}
			lastPlotLine = plot;
			plotLines.push({
				color : '#FF0000',
				zIndex : 2,
				width : 1,
				value : plot.value,
				label : {
					text : plot.text,
					y : yaxis
				}
			});
		});

		return plotLines;
	};

	this._maxEventTime = null;
	this.maxEventTime = function() {
		if (this._maxEventTime) {
			return this._maxEventTime;
		}
		var maxEventTime = 0;
		this.allEvents().forEach(function(e) {
			maxEventTime = Math.max(maxEventTime, e.eventTime);
		});

		this._maxEventTime = maxEventTime;
		return maxEventTime;
	};

	this.appendCommonEvent = function(event) {
		this.commonEvents.push(event);
	};

	this.appendConsumerEvent = function(event) {
		var groupId = event.datas.groupId;
		var topic = event.datas.topic;
		if (topic != this.topicName) {
			return;
		}
		if (event.eventType == "Message.BizProcessEnd") {
			var groupEvents = this.consumerEndEvents[groupId];
			if (!groupEvents) {
				groupEvents = [];
				this.consumerEndEvents[groupId] = groupEvents;
			}
			groupEvents.push(event);
		} else if (event.eventType == "Message.BizProcessStart") {
			var groupEvents = this.consumerStartEvents[groupId];
			if (!groupEvents) {
				groupEvents = [];
				this.consumerStartEvents[groupId] = groupEvents;
			}
			groupEvents.push(event);
		} else {
			var groupEvents = this.consumerEvents[groupId];
			if (!groupEvents) {
				groupEvents = [];
				this.consumerEvents[groupId] = groupEvents;
			}
			groupEvents.push(event);
		}
	};

	this._allEvents = null;
	this.allEvents = function() {
		if (this._allEvents) {
			return this._allEvents;
		}
		var allEvents = this.commonEvents;
		for (consumerId in this.consumerEvents) {
			allEvents = allEvents.concat(this.consumerEvents[consumerId]);
		}

		this._allEvents = allEvents;
		return allEvents;
	};

	this.eventTimeRange = function() {
		var allEvents = this.allEvents();
		return d3.extent(allEvents, function(event) {
			return event.eventTime;
		});
	};

	this.categories = function() {
		var categories = [];
		categories.push("Producer@" + this.commonEvents[1].datas.producerIp);

		for (groupId in this.consumerEvents) {
			var ce = this.consumerEvents[groupId][0];
			categories.push("<b>" + groupId + "@" + ce.datas.consumerIp
					+ "</b>");
		}

		return categories;
	};

	this.plot = function() {
		var cse = this.consumerEvents;
		for (cid in cse) {
			cse[cid].sort(function(a, b) {
				return a.eventTime - b.eventTime;
			});
		}
		for (cid in this.consumerStartEvents) {
			this.consumerStartEvents[cid].sort(function(a, b) {
				return a.eventTime - b.eventTime;
			});
		}
		for (cid in this.consumerEndEvents) {
			this.consumerEndEvents[cid].sort(function(a, b) {
				return a.eventTime - b.eventTime;
			});
		}
		for (cid in cse) {
			var singleConsumerEvents = cse[cid];
			var count = 0;
			for (var i = 0; i < singleConsumerEvents.length; i++) {
				if (singleConsumerEvents[i].eventType == "Message.Acked") {
					singleConsumerEvents.splice(i, 0,
							this.consumerStartEvents[cid][count],
							this.consumerEndEvents[cid][count]);
					count++;
					i = i + 2;
					var startTime = singleConsumerEvents[i - 2].eventTime;
					var endTime = singleConsumerEvents[i - 1].eventTime;
					var deliverTime = singleConsumerEvents[i - 3].eventTime;
					var ackTime = singleConsumerEvents[i].eventTime;
					var processTime = endTime - startTime;
					if (processTime <= 0) {
						processTime = 20;
					}
					if (startTime < deliverTime || endTime < deliverTime
							|| startTime > ackTime || endTime > ackTime) {
						singleConsumerEvents[i - 2].realEventTime = singleConsumerEvents[i - 2].eventTime;
						singleConsumerEvents[i - 2].eventTime = (ackTime
								- deliverTime - processTime)
								/ 2 + deliverTime;
						singleConsumerEvents[i - 1].realEventTime = singleConsumerEvents[i - 1].eventTime;
						singleConsumerEvents[i - 1].eventTime = singleConsumerEvents[i - 2].eventTime
								+ processTime;
					}
				}
			}
		}
		var cme = this.commonEvents;
		var lastEvent;
		for ( var i in cme) {
			var curEvent = cme[i];
			curEvent.x = curEvent.eventTime - this.bornTime;
			if (lastEvent) {
				lastEvent.width = curEvent.x - lastEvent.x;
			}
			lastEvent = curEvent;
		}
		var fstDeliverTime = Infinity;
		for ( var cid in cse) {
			var dt = cse[cid][0].eventTime;
			fstDeliverTime = Math.min(fstDeliverTime, dt);
		}
		lastEvent.width = fstDeliverTime - lastEvent.eventTime;
		var lastCommonEvent = lastEvent;

		this.maxConsumerEventCount = 0;
		for ( var cid in cse) {
			var events = cse[cid];
			if (events.length > this.maxConsumerEventCount) {
				this.maxConsumerEventCount = events.length;
			}
			lastEvent = lastCommonEvent;
			for (i in events) {
				var event = events[i];
				event.x = event.eventTime - this.bornTime;
				if (i > 0) {
					lastEvent.width = event.x - lastEvent.x;
				}
				lastEvent = event;
			}
		}
	};
}

angular
		.module('hermes-tracer', [ 'ngResource' ])
		.controller(
				'tracer-controller',
				[
						'$scope',
						'$resource',
						function(scope, resource) {
							scope.msg_date = new Date();
							scope.show_message = function show_message(refKey,
									msg_date) {
								var esIndex = "logstash-"+ msg_date.getFullYear()+ '.'
										+ ('0' + (msg_date.getMonth() + 1)).slice(-2) + '.'
										+ ('0' + msg_date.getDate()).slice(-2);
								var esServer = "http://" + esUrl + ":9200/";
								//var esServer = "http://10.2.27.123:9200/"
								var refKeySearchUrl = esServer + esIndex
										+ '/biz/_search?q=datas.refKey:"'
										+ refKey + '"';
								get(
										refKeySearchUrl,
										function(data) {
											if (data.hits.total == 0) {
												show_op_info.show("RefKey "+refKey+" Not Found!",false);
												return;
											}
											var hits = data.hits.hits;
											var eventMap = {};
											hits.forEach(function(hit) {
												var event = hit._source;
												eventMap[event.eventType] = event;
											});
											var trace = new Trace();
											trace.bornTime = eventMap["Message.Received"].datas.bornTime;
											if (trace.bornTime >= eventMap["Message.Received"].eventTime) {
												trace.realBornTime = trace.bornTime;
												trace.bornTime = eventMap["Message.Received"].eventTime - 20;
											}

											// common events
											trace.appendCommonEvent({
												eventTime : trace.bornTime,
												eventType : "Message.Born"
											});
											trace.appendCommonEvent(eventMap["Message.Received"]);
											trace.appendCommonEvent(eventMap["Message.Saved"]);
											trace.topicName = eventMap["Message.Received"].datas.topic;
											var transformEvent = eventMap["RefKey.Transformed"];

											var msgIdSearchUrl = esServer
													+ esIndex
													+ "/biz/_search?size=20&q=datas.msgId:"
													+ transformEvent.datas.msgId;
											get(
													msgIdSearchUrl,
													function(data) {
														var hits = data.hits.hits;
														hits.forEach(function(hit) {
																	var event = hit._source;
																	if (/^Message/.test(event.eventType)) {
																		trace.appendConsumerEvent(event);
																	}
																});
														visualize(trace);
													});
											console.log(trace);
										});

								function visualize(trace) {
									trace.plot();
									var eventTimeRange = trace.eventTimeRange();
									scale = d3.scale.linear().domain([0,eventTimeRange[1] - trace.bornTime ]).range([ 0, 400 ]);
									cscale = d3.scale.linear().domain([	0,	eventTimeRange[1]- trace.bornTime ]).range([ "blue", "red" ]);

									var allEvents = trace.allEvents();
									$('#container').highcharts(
											{	chart : {
													type : 'columnrange',
													inverted : true
												},
												title : {
													text : 'Message trace'
												},
												subtitle : {
													text : refKey
												},
												xAxis : {
													categories : trace
															.categories()
												},
												yAxis : {
													title : {
														text : "Millisecond since "+ new Date(trace.commonEvents[0].eventTime).toISOString()
													},
													min : 0,
													max : trace.maxEventTime()- trace.bornTime+ 10,
													minColor : "#ff0000",
													plotLines : trace.plotLines()
												},
												tooltip : {
													valueSuffix : ''
												},
												plotOptions : {
													columnrange : {
														dataLabels : {
															enabled : true,
															formatter : function() {
																return this.y;
															}
														}
													}
												},
												legend : {
													enabled : false
												},
												series : trace.series(),
											});
								}
							};
						} ]);