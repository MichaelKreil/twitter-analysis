"use strict";

const fs = require('fs');
const zlib = require('zlib');
const utils = require('../../lib/utils.js');
const colors = require('colors');
const scraper = (require('../../lib/scraper.js'))('hashtag');
const resolve = require('path').resolve;
const parseQuery = (url => require('querystring').parse(require('url').parse(url).query));

var dates = [
	'2017-12-22',
	'2017-12-23',
	'2017-12-24',
	'2017-12-25',
	'2017-12-26',
	'2017-12-27',
	'2017-12-28',
	'2017-12-29',
	'2017-12-30',
	'2017-12-31',
	'2018-01-01',
]

var hashtags = [
	{name: '34C3', query: '34c3'},
	{name: 'NetzDG', query: 'NetzDG'},
	{name: 'Silvester', query: 'Silvester'},
	{name: 'IranProtests', query: 'تظاهرات_سراسری OR IranProtests'},
]


hashtags.forEach(obj => {
	dates.forEach(date => {
		runScraper(obj.name, obj.query, date);
	})
})

scraper.run();

function runScraper(name, query, date) {
	var filename = resolve(__dirname, '../../data/hashtags/'+name+'/'+name+'_'+date+'.jsonstream');

	if (fs.existsSync(filename) || fs.existsSync(filename+'.7z') || fs.existsSync(filename+'.gz')) {
		console.log(colors.yellow('Ignore "'+name+'" "'+date+'"'));
		return;
	}

	utils.ensureDir(filename);


	var tweets = new Map();

	var task = scraper.getSubTask()

	scrape();

	task.finished(() => {
		tweets = Array.from(tweets.values());
		if (tweets.length === 0) return;

		tweets.sort((a,b) => a.id_str < b.id_str ? -1 : 1);

		tweets = tweets.map(t => t.buffer);

		var firstSave = false;

		while (tweets.length > 0) {
			var buf = Buffer.concat(tweets.slice(0,300000));
			tweets = tweets.slice(300000);

			if (firstSave) {
				fs.writeFileSync(filename, buf);
				firstSave = false;
			} else {
				fs.appendFileSync(filename, buf);
			}
		}
	})

	function scrape(max_id) {
		var attributes = {q:query, result_type:'recent', count:100, max_id:max_id};

		var minDate = new Date(date);
		var maxDate = new Date(minDate.getTime()+86400000);
		attributes.until = maxDate.toISOString().substr(0,10);

		task.fetch(
			'search/tweets',
			attributes,
			result => {
				result.statuses = result.statuses.filter(t => {
					var d = new Date(t.created_at);
					if (d < minDate) return false;
					if (d > maxDate) return false;
					return true;
				})

				result.statuses.forEach(t => tweets.set(t.id_str, {
					id_str: t.id_str,
					created_at: t.created_at,
					buffer: Buffer.from(JSON.stringify(t)+'\n', 'utf8')
				}));

				var min_id = utils.getTweetsMinId(result.statuses);

				if (min_id) {
					console.log([
						'next_id: '+min_id,
						'date: '+(result.statuses[0]||{}).created_at,
						'count: ' + tweets.size
					].join('\t'));

					scrape(min_id);
				}
			}
		)
	}
}

