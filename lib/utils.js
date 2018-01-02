"use strict"

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const colors = require('colors');

module.exports = {
	readJSON: readJSON,
	ao2oa: arrayOfObjects2ObjectOfArray,
	aao2oaa: arrayOfArrayOfObjects2ObjectOfArrayOfArray,
	analyseJSON: analyseJSON,
	ensureDir: ensureDir,
	getTweetsMinId: getTweetsMinId,
}

function readJSON(filename, cbLine, cbFinished) {
	const maxSize = 64*1024*1024;
	const timeStep = 5*1000;

	var size = fs.statSync(filename).size;
	var file = fs.openSync(filename, 'r');
	var buffer = new Buffer(maxSize);
	var bufferOffset = 0;
	var fileOffset = 0;
	var nextTime = Date.now()+timeStep;

	read()

	function read() {
		if (Date.now() > nextTime) {
			nextTime = Date.now()+timeStep;
			console.log(colors.grey('   '+(100*fileOffset/size).toFixed(0)+'%'));
		}

		var readBytes = maxSize-bufferOffset;
		
		fs.read(file, buffer, bufferOffset, readBytes, fileOffset, (err, bytesRead) => {
			fileOffset += bytesRead;
			var isFinished = (bytesRead < readBytes);

			var lines = buffer.toString('utf8', 0, bufferOffset+bytesRead);
			lines = lines.split('\n');

			if (!isFinished) {
				bufferOffset = buffer.write(lines.pop(), 0);
			}

			if (lines.length === 0) throw Error('maxSize ('+maxSize+') is too small');
			lines.forEach(l => {
				if (l.length === 0) return;
				try {
					var l = JSON.parse(l);
				} catch (e) {
					console.dir(l);
					throw e;
				}
				cbLine(l)
			});

			if (isFinished) {
				setTimeout(cbFinished, 0);
				fs.closeSync(file);
			} else {
				setTimeout(read, 0);
			}
		})
	}
}

function arrayOfObjects2ObjectOfArray (list) {
	if (list.length === 0) return null;
	var keys = new Set();
	list.forEach(
		entry => Object.keys(entry).forEach(
			key => keys.add(key)
		)
	);
	var obj = {};
	Array.from(keys.values()).forEach(key => {
		obj[key] = list.map(entry => entry.hasOwnProperty(key) ? entry[key] : null);
	})
	return obj;
}

function arrayOfArrayOfObjects2ObjectOfArrayOfArray (list) {
	if (list.length === 0) return null;
	var keys = new Set();
	list.forEach(
		sublist => sublist.forEach(
			entry => Object.keys(entry).forEach(
				key => keys.add(key)
			)
		)
	)
	var obj = {};
	Array.from(keys.values()).forEach(key => {
		obj[key] = list.map(sublist =>
			sublist.map(
				entry => entry.hasOwnProperty(key) ? entry[key] : null
			)
		)
	})
	return obj;
}

function analyseJSON(data) {
	var text = JSON.stringify(data);

	return {
		data: scanRec(data),
		sum: formatSize(text.length)+' -> '+formatSize(zlib.deflateRawSync(text).length)
	}

	function scanRec(data) {
		if (Array.isArray(data)) return analyseSize(JSON.stringify(data));
		if (typeof data !== 'object') throw Error();

		var obj = {};
		Object.keys(data).forEach(key => obj[key] = scanRec(data[key]));
		return obj;
	}

	function analyseSize(text) {
		return formatSize(text.length)+' -> '+formatSize(zlib.deflateRawSync(text).length)
	}
	
	function formatSize(value) {
		return Math.round(100*value/(1024))/100;
	}
}

function getTweetsMinId(tweets) {
	if (tweets.length === 0) return false;
	var min_id = false;
	tweets.forEach(t => min_id = min_id ? minId(min_id, t.id_str) : t.id_str);
	return dec(min_id);

	function minId(a,b) {
		if (a.length !== b.length) return (a.length < b.length) ? a : b;
		return (a < b) ? a : b;
	}

	function dec(a) {
		a = a.split('');
		var i = a.length-1;

		while (i >= 0) {
			switch (a[i]) {
				case '9': a[i] = '8'; return a.join('');
				case '8': a[i] = '7'; return a.join('');
				case '7': a[i] = '6'; return a.join('');
				case '6': a[i] = '5'; return a.join('');
				case '5': a[i] = '4'; return a.join('');
				case '4': a[i] = '3'; return a.join('');
				case '3': a[i] = '2'; return a.join('');
				case '2': a[i] = '1'; return a.join('');
				case '1': a[i] = '0'; return a.join('');
				case '0': a[i] = '9'; i--;
			}
		}
		throw Error();
	}
}

function ensureDir(file) {
	var directory = path.dirname(file);
	if (!fs.existsSync(directory)) {
		ensureDir(directory);
		fs.mkdirSync(directory);
	}
}

