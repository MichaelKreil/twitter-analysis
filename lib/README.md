# lib

## scraper.js

- is the main scraper entry point
- uses `twitter-wrapper.js`

## twitter-wrapper.js

- manages all twitter requests, error handling, rate limits, load balancing, ...  
- uses <https://www.npmjs.com/package/twitter> for request. But since it was necessary to hack that module, I copy-paste-hacked it to `twitter.js`

## twitter.js & parse.js

- makes the actual twitter request.
- To be honest: I can't remember, what I hacked. Has propably something to do with extracting rate limits form HTTP responses?!?! Maybe not ... Should make a diff to check ...
