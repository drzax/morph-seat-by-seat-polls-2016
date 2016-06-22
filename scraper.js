"use strict";

// This is a template for a Node.js scraper on morph.io (https://morph.io)
var fs = require('fs');
var path = require('path');
var cheerio = require("cheerio");
var request = require("request");
var moment = require('moment');
var sqlite3 = require("sqlite3").verbose();
var qs = require('querystring');

var db;

// Delete existing data
try {
	fs.unlinkSync(path.join(__dirname, 'data.sqlite'));
} catch(e) {}

// Setup the DB
db = new Promise((resolve, reject) => {
	var conn = new sqlite3.Database("data.sqlite");
	conn.serialize(() => {
		conn.run(`CREATE TABLE IF NOT EXISTS data
				(
					electorateName TEXT,
					electorateCode TEXT,
					date TEXT,
					party1code TEXT,
					party1pct TEXT,
					party2code TEXT,
					party2pct TEXT,
					reference TEXT
				)`, (err) => err ? reject(err) : resolve(conn));
	});
});


url('https://en.wikipedia.org/wiki/Opinion_polling_for_the_Australian_federal_election,_2016')
	.then((html) => {
		var $, polls = [];
		$ = cheerio.load(html);

		// Collect relevant tables
		// console.log($('#Individual_seat_polling_during_the_election_campaign').parent('h2').nextUntil('h2', 'table'));
		$('#Individual_seat_polling_during_the_election_campaign').parent('h2').nextUntil('h2', 'table').each(function () {

			var data = [];

			$(this).find('tr').each(function(row) {
				data[row] = [];
				$(this).find('td,th').each(function (col) {
					data[row][col] = $(this).text();
				});
			});

			for (let i=3; i<data.length; i++) {
				let poll = {};
				let row = data[i];

				// Compile data for this poll
				poll.$date = moment(row[0].replace(/^.*\–/,''), 'D MMM YYYY').toISOString();
				poll.$electorateName = row[1].split(/ [\(\[]/)[0];
				// poll.$reference =
				var refId = $(this).find('tr').eq(i).find('td').eq(0).find('a').eq(1).attr('href');
				poll.$reference = $(refId).find('.reference-text a').attr('href');

				var partyCol = 2;
				var party = 1;
				while (party<3) {
					if (row[partyCol].trim().length) {
						poll[`$party${party}code`] = data[2][partyCol-1].trim();
						poll[`$party${party}pct`] = row[partyCol].trim().replace('%','');
						party++;
					}
					partyCol++;
				}

				// Put it in the global list
				polls.push(poll);
			}

		});
		return polls;
	}).then((polls) => {
		return url('https://api.morph.io/drzax/morph-australian-federal-election-electorates-2016/data.json?'+qs.stringify({
			key: process.env.MORPH_KEY,
			query: "SELECT * FROM data"
		}))
			.then(JSON.parse)
			.then((electorates)=>{
				polls.forEach((poll) => {
					poll.$electorateCode = electorates.find((electorate) => electorate.electorateName === poll.$electorateName).electorateCode;
				});
				return polls;
			});
	})
	.then((polls) => {
		db.then(function(db) {
			polls.forEach((poll) => {
				db.run("INSERT INTO data (electorateName, electorateCode, date, party1code, party1pct, party2code, party2pct, reference) VALUES ($electorateName, $electorateCode, $date, $party1code, $party1pct, $party2code, $party2pct, $reference)", poll, (global.gc) ? global.gc : null);
			});
		}, handleErr);
	})
	.catch(handleErr);

function url(url) {
	return new Promise((resolve, reject) => {
		request(url, function (err, res, body) {
			if (err) return reject(err);
			if (res.statusCode !== 200) return reject(new Error(`Error fetching URL ${url}`));
			resolve(body);
		});
	});
}

function handleErr(err) {
	setImmediate(()=>{
		throw err;
	});
}
