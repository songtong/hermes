var show_op_info = function() {
	"use strict";

	var info_elem, hideHandler, that = {};
	that.init = function(options) {
		info_elem = $(options.selector);
	};
	that.show = function(text) {
		clearTimeout(hideHandler);
		info_elem.find("span").html(text);
		info_elem.delay(200).fadeIn().delay(4000).fadeOut();
	};
	return that;
}();

var substringMatcher = function(strs) {
	return function findMatches(q, cb) {
		var matches, substringRegex;
		matches = [];
		substrRegex = new RegExp(q, 'i');
		$.each(strs, function(i, str) {
			if (substrRegex.test(str)) {
				matches.push(str);
			}
		});
		cb(matches);
	};
};

function collect_schemas(data, schema, reverse) {
	var ret = [];
	for (var i = 0; i < data.length; i++) {
		ret.push(data[i][schema]);
	}
	ret.sort();
	if (reverse) {
		ret.reverse();
	}
	return ret;
}

function unique_array(array) {
	var s = new Set(array);
	var l = [];
	s.forEach(function(value) {
		l.push(value);
	});
	return l;
}

$(function() {
	show_op_info.init({
		"selector" : ".op-alert"
	});
});
