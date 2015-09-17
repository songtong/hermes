var show_op_info = function() {
	"use strict";

	var info_elem, hideHandler, that = {};
	that.init = function(options) {
		info_elem = $(options.selector);
	};
	that.show = function(text, success) {
		clearTimeout(hideHandler);
		info_elem.attr('class', 'op-alert alert alert-' + (success ? 'success' : 'danger'));
		info_elem.find("#op_info").html(text);
		info_elem.delay(200).fadeIn().delay(5000).fadeOut();
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

function starts_with(source, target) {
	if (source.length < target.length) {
		return false;
	}
	var matched = 0;
	for (var idx = 0; idx < target.length; idx++) {
		if (source[idx] == target[idx]) {
			matched = matched + 1;
		} else {
			break;
		}
	}
	return matched == target.length;
}

function ends_with(source, target) {
	if (source.length < target.length) {
		return false;
	}
	var matched = 0;
	for (var idx = 0; idx < target.length; idx++) {
		if (source[source.length - target.length + idx] == target[idx]) {
			matched = matched + 1;
		} else {
			break;
		}
	}
	return matched == target.length;
}

$(function() {
	show_op_info.init({
		"selector" : ".op-alert"
	});
});
