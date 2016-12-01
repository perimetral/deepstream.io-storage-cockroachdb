'use strict'

const events = require('events');
const util = require('util');
const pckg = require('../package.json');
const defaults = require('../defaults.js');

const pg = require('pg');

const dbGenerator = (options, self) => {
	let db = new pg.Client(options.cockroach);
	db.end();
	db.connect((e) => {
		if (e) return self.emit('error', e);
		db.query(`create database if not exists ${options.dbName}`, (e, result) => {
			if (e) return self.emit('error', e);
			db.query(`set database = ${options.dbName}`, (e, result) => {
				if (e) return self.emit('error', e);
				db.query(`create table if not exists ${options.tableName} (ds_key ${options.keyType}, ds_value ${options.valueType})`, (e, result) => {
					if (e) return self.emit('error', e);
					db.query('show tables', (e, data) => {
						if (e) return self.emit('error', e);
						data.rows.forEach((x, i, ar) => { self._tableList.push(x.Table); });
					});
				});
				self.isReady = true;
				self.emit('ready');
			});
		});
	});
	db.on('error', (e) => { self.emit('error', e); });
	return db;
};

class Connector extends events.EventEmitter {
	constructor (options) {
		super();
		if (! (typeof options === 'object')) throw new TypeError('Incorrect connection options passed');
		this.isReady = false;
		this.name = pckg.name;
		this.version = pckg.version;
		options = Object.assign({}, defaults, options);
		if (process.env.ds_cockroach) options.cockroach = process.env.ds_cockroach;
		if (process.env.ds_dbName) options.dbName = process.env.ds_dbName;
		if (process.env.ds_tableName) options.tableName = process.env.ds_tableName;
		if (process.env.ds_keyType) options.keyType = process.env.ds_keyType;
		if (process.env.ds_valueType) options.valueType = process.env.ds_valueType;
		if (process.env.ds_splitter) options.splitter = process.env.ds_splitter;
		this.options = Object.assign({}, options);
		this._dbName = options.dbName;
		this._tableName = options.tableName;
		this._keyType = options.keyType;
		this._valueType = options.valueType;
		this._splitter = options.splitter;
		this._tableList = [];
		this._db = dbGenerator(options, this);
	}

	_upsert (tableName, key, value) {
		return new Promise((go, stop) => {
			this._db.query(`select * from ${tableName} where ds_key = $1`, [ key ], (e, rows) => {
				if (e) return stop(e);
				if (rows[0]) this._db.query(`update ${tableName} set ds_value = $1 where ds_key = $2`, [ value, key ], (e, rows) => {
					if (e) return stop(e);
					return go();
				});
				if (! rows[0]) this._db.query(`insert into ${tableName} (ds_key, ds_value) values ($1, $2)`, [ key, JSON.stringify(value) ], (e, rows) => {
					if (e) return stop(e);
					return go();
				});
			});
		});	
	}

	set (key, value, callback) {
		let splitted = undefined;
		try {
			 splitted = key.split(this._splitter);
		} catch (e) { return callback(e); };
		let tableName = (splitted.length > 1) ? splitted[0] : this._tableName;
		if (this._tableList.includes(tableName)) this._upsert(tableName, key, value).then(() => { return callback(null); }, (e) => { return callback(e); });
		else {
			let keyType = this._keyType;
			let valueType = this._valueType;
			this._db.query(`create table if not exists ${tableName} (ds_key ${keyType}, ds_value ${valueType})`, (e, result) => {
				if (e) return this.emit('error', e);
				this._upsert(tableName, key, value).then(() => { return callback(null); }, (e) => { return callback(e); });
			});
		};
	}

	get (key, callback) {
		let splitted = undefined;
		try {
			 splitted = key.split(this._splitter);
		} catch (e) { return callback(e); };
		let tableName = (splitted.length > 1) ? splitted[0] : this._tableName;
		this._db.query(`select * from ${tableName} where ds_key = $1`, [ key ], (e, data) => {
			if (e) return callback(e);
			if (data.rows.length < 1) return callback(null, null);
			let result = data.rows[0].ds_value;
			try {
				let deserialized = JSON.parse(result);
				return callback(null, deserialized);
			} catch (e) {
				return callback(null, result);
			};
		});
	}

	delete (key, callback) {
		let splitted = undefined;
		try {
			 splitted = key.split(this._splitter);
		} catch (e) { return callback(e); };
		let tableName = (splitted.length > 1) ? splitted[0] : this._tableName;
		this._db.query(`delete from ${tableName} where ds_key = $1`, [ key ], (e, rows) => {
			if (e) callback(e);
			return callback(null);
		});
	}
}

module.exports = Connector;