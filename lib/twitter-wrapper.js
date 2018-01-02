"use strict"

const fs = require('fs');
const path = require('path');
const colors = require('colors');
const Levelup = require('level');
const Twitter = require('./twitter');

console.error(colors.grey('db open'));
var db;

process.on('beforeExit', () => {
	if (db.isClosed()) return;
	db.close(() => {
		console.error(colors.grey('db closed'));
	});
})

var config = require('../config/config.js');

var accounts = new Accounts();
var tokens = fs.readFileSync(path.resolve(__dirname, '../config/tokens.tsv'), 'utf8').split('\n');
tokens.forEach(line => {
	line = line.split('\t');
	accounts.add(line[0], line[1], line[2], line[3]);
})

function fetch(type, params, cbFetch) {
	var bigResult, useIdContainer = false;
	fetchSingle(type, params, parseResult);

	function parseResult(result) {
		if (!result) {
			cbFetch(false);
			return
		}

		if (!bigResult) {
			bigResult = result;
		} else {
			Object.keys(result).forEach(key => {
				switch (key) {
					case 'ids':
						if (!useIdContainer) {
							bigResult.ids = [Buffer.from(bigResult.ids.join(','),'ascii')];
							useIdContainer = true
						}
						bigResult.ids.push(Buffer.from(result.ids.join(','),'ascii'))
					break;
					case 'next_cursor':
					case 'next_cursor_str':
					case 'previous_cursor':
					case 'previous_cursor_str':
						bigResult[key] = result[key];
					break;
					default:
						throw Error(key);
				}
			})
		}

		var cursor = result.next_cursor_str;

		if (!cursor || cursor === '0') {
			if (useIdContainer) {
				bigResult.ids = bigResult.ids.map(buf => buf.toString('ascii').split(','))
				bigResult.ids = Array.prototype.concat.apply([],bigResult.ids);
			}

			cbFetch(bigResult);
		} else {
			params.cursor = cursor;
			fetchSingle(type, params, parseResult);
		}
	}
}

function fetchSingle(type, params, cbFetch) {
	var keys = Object.keys(params);
	keys.sort();
	var id = type+' & '+keys.map(key => key+'='+params[key]).join(' ');

	if (db.isClosed()) return; // leave in limbo

	db.get(id, (err, value) => {
		if (err && err.notFound) {
			var msg = 'fetch "'+id+'"';
			if (msg.length > 140) msg = msg.substr(0,140)+'...';
			//console.error(colors.gray(msg));
			
			twitterFetch(type, params, (result) => {
				if (db.isClosed()) return; // leave in limbo
				db.put(id, result, () => {
					cbFetch(result);
				})
			})
		} else {
			cbFetch(value)
		}
	});
}

function twitterFetch(type, params, cbFetch) {
	accounts.get(type, (account, freeAccount) => {
		account.client.get(type, params, (error, result, response) => {

			function finish(error) {
				if (error) {
					fs.appendFileSync('error.log', JSON.stringify(error)+'\n', 'utf8');
				} else {
					freeAccount();
				}
			}

			function tryAgain(seconds) {
				setTimeout(() => { twitterFetch(type, params, cbFetch) }, seconds*1000);
			}

			if (response && response.headers) {
				account.setRateLimit(type, parseInt(response.headers['x-rate-limit-remaining'], 10));
				account.setReset(          parseInt(response.headers['x-rate-limit-reset'    ], 10));
			}

			if (error && (error.code === 'ENOTFOUND') && (error.syscall === 'getaddrinfo')) {
				console.error(colors.red('Shit, network trouble, "'+error.hostname+'" not found.'));
				finish();
				return tryAgain(10);
			}

			if (!response) {
				console.error(colors.red('no response'));
				finish();
				return tryAgain(10);
			}

			if (!response.headers) {
				console.dir(response, {colors:true, depth:2});
				console.dir(result, {colors:true});
				console.dir(error, {colors:true});
				throw Error('no response headers');
			}

			if (error) {
				try {
					error[0].name = account.client.options.name;
					error[0].secret = account.client.options.access_token_secret.substr(0,8);
				} catch (e) {};

				if (result.error === 'Not authorized.') {
					console.error(colors.yellow('Not authorized.'))
					finish();
					return cbFetch(result);
				}

				if (error[0]) {
					if (error[0].code === 17) {
						console.warn(colors.yellow('User does not exist "'+JSON.stringify(params)+'"'))
						finish();
						return cbFetch(false);
					}
					if (error[0].code === 34) {
						console.warn(colors.yellow('Page does not exist "'+type+'" "'+JSON.stringify(params)+'"'))
						finish();
						return cbFetch(false);
					}
					if (error[0].code === 50) {
						console.warn(colors.yellow('Requesting data of unknown account, when "'+JSON.stringify(params)+'"'))
						finish();
						return cbFetch(false);
					}
					if (error[0].code === 63) {
						console.warn(colors.yellow('Requesting data of suspended account, when "'+JSON.stringify(params)+'"'))
						finish();
						return cbFetch(false);
					}
					if (error[0].code === 88) {
						console.warn(colors.yellow('Rate Limit exceeded'))
						finish();
						return tryAgain(1);
					}
					if (error[0].code === 89) {
						console.error(colors.red('Token invalid from "'+account.client.options.name+'" ('+account.client.options.access_token_secret.substr(0,8)+'...)'))
						finish(error);
						return tryAgain(1);
					}
					if (error[0].code === 130) {
						console.error(colors.red('Twitter is over capacity :/'))
						finish();
						return tryAgain(20);;
					}
					if (error[0].code === 131) {
						console.error(colors.red('Twitter has an internal error'))
						finish();
						return tryAgain(20);
					}
					if (error[0].code === 136) {
						console.error(colors.red('You have been blocked from viewing this user\'s profile.'))
						finish();
						return tryAgain(1);
					}
					
					if (error[0].code === 179) {
						console.error(colors.red('Sorry, you are not authorized to see this status.'))
						finish();
						return cbFetch(false);
					}
					if (error[0].code === 200) {
						console.error(colors.red('Something is forbidden.'));
						finish();
						return cbFetch(false);
					}
					if (error[0].code === 326) {
						console.error(colors.red('Account is temporarily disabled'));
						finish(error);
						return tryAgain(1);
					}
				}

				if (response.statusCode === 503) {
					console.error(colors.red('Service Temporarily Unavailable'))
					finish();
					return tryAgain(10);
				}

				console.error(colors.red('Unknown Error!'));
				console.error(colors.red('response:')); console.dir(response, {colors:true, depth:2});
				console.error(colors.red('client:'));   console.dir(account.client, {colors:true, depth:3});
				console.error(colors.red('error:'));    console.dir(error, {colors:true, depth:2});
				finish(error);
				return tryAgain(10);
				
			}
			finish();
			cbFetch(result);
		})
	})
}

module.exports = {
	initDB:name => db = Levelup(path.resolve(__dirname, '../cache/'+name), { cacheSize: 1024*1024*1024, valueEncoding : 'json' }),
	fetch:fetch
}

function Accounts() {
	var me = {};

	var requestTypes = [
		// source: https://dev.twitter.com/rest/public/rate-limits
		{key:'favorites/list',          rateLimit:   75 },
		{key:'followers/ids',           rateLimit:   15 },
		{key:'friends/ids',             rateLimit:   15 },
		{key:'friendships/show',        rateLimit:   15 },
		{key:'lists/memberships',       rateLimit:   75 },
		{key:'search/tweets',           rateLimit:  450 },
		{key:'statuses/lookup',         rateLimit:  300 },
		{key:'statuses/retweeters/ids', rateLimit:  300 },
		{key:'statuses/retweets',       rateLimit:  300 },
		{key:'statuses/user_timeline',  rateLimit: 1500 },
		{key:'users/lookup',            rateLimit:  300 },
		{key:'users/show',              rateLimit:  900 },
	];

	var accounts = [];
	var accountsByType = {};
	requestTypes.forEach(type => accountsByType[type.key] = [])

	me.add = function (id, name, key, secret) {
		var account = new Account(name, key, secret);
		accounts.push(account);
		requestTypes.forEach(type => accountsByType[type.key].push(account));
	}

	me.get = function (type, cb) {
		var list = accountsByType[type];

		if (list.length <= 0) throw Error();

		list.sort((a,b) => {
			if (a.rateLimit(type) !== b.rateLimit(type)) return b.rateLimit(type) - a.rateLimit(type);
			if (a.reset !== b.reset) return a.reset - b.reset;
			return Math.random()*2-1;
		});

		//if (type === 'users/show') console.error(list.map(a => [a.reset, a.rateLimit('users/show')].join('\t')).join('\n'))

		var account = list.shift();

		var timeToWait = -1;
		if (account.rateLimit(type) < 1) timeToWait = 5 + account.reset - Date.now()/1000;
		
		account.decRateLimit(type);

		if (timeToWait <= 0) {
			cb(account, () => list.push(account))
		} else {
			var time = (timeToWait/60).toFixed(0) + ':' + (100+(timeToWait % 60)).toFixed(0).substr(1);
			console.warn(colors.yellow('Have to wait '+time+' for @'+account.name));
			setTimeout(() => {
				cb(account, () => list.push(account))
			}, timeToWait*1000)
		}
	}

	return me;

	function Account(name, key, secret) {
		var reset = Date.now()/1000 + 15*60;
		var client = new Twitter({
			consumer_key:config.twitter_keys.consumer_key,
			consumer_secret:config.twitter_keys.consumer_secret,
			access_token_key:key,
			access_token_secret:secret,
			name:name
		})

		var rateLimit = {};
		requestTypes.forEach(type => rateLimit[type.key] = type.rateLimit);

		var me = {
			get name () { return name },
			get client () { return client },
			get reset () { return reset },
			setReset: function (v) { reset = v; if (v > Date.now()/1000 + 15*60 + 1) throw Error(v+' vs '+Date.now()/1000); },
			rateLimit: function (type) { return rateLimit[type] },
			setRateLimit: function (type, v) { rateLimit[type] = Math.max(0, v) },
			decRateLimit: function (type) {
				if (rateLimit[type] <= 0) {
					rateLimit[type] = 0;
				} else {
					rateLimit[type]--;
				}
			}
		}

		return me;
	}
}