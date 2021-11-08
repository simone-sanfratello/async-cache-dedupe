'use strict'

const { test } = require('tap')
const { Cache } = require('..')
const stringify = require('safe-stable-stringify')

const { kValues } = require('../symbol')

const dummyStorage = {
  async get (key) { },
  async set (key, value, ttl, references) { },
  async remove (key) { },
  async invalidate (references) { },
  async clear () { },
  async refresh () { }
}

// TODO cache.get, set, invalidate, clear
