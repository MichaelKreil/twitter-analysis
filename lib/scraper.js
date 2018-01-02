"use strict"

const twitter = require('./twitter-wrapper.js');

function Scraper(name) {
	if (!name) throw Error('Scraper needs a name');
	twitter.initDB(name);

	const maxCount = 16;
	var tasks = [];
	var running = 0;

	function next() {
		while ((running < maxCount) && (tasks.length > 0)) {
			(() => {
				var task = tasks.shift();
				running++;
				setImmediate(() => {
					task(() => {
						running--;
						next();
					})
				})
			})();
		}
	}

	return TaskGroup();

	function TaskGroup() {
		var me = {};
		var finished = [];
		var taskSet = new Set();
		
		me.getSubTask = function () {
			let group = TaskGroup();
			taskSet.add(group);
			group.finished(() => {
				taskSet.delete(group);
				check();
			})
			return group;
		}

		me.fetch = function (type, params, cbFetch) {
			let task = function (cbTask) {
				twitter.fetch(type, params, (result) => {
					if (cbFetch) cbFetch(result);
					taskSet.delete(task);
					check();
					cbTask();
				});
			}
			taskSet.add(task);
			tasks.push(task);
			next();
		}

		me.finished = function (cb) {
			finished.push(cb);
		}

		function check() {
			if ((taskSet.size === 0) && (finished.length > 0)) {
				var cb = finished.pop();
				setImmediate(() => {
					cb();
					check();
				})
			}
		}

		me.run = next;
		me.check = check;
		
		return me;
	}
}

module.exports = Scraper;
