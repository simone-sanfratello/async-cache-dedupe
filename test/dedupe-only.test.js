'use strict'

const t = require('tap')
const { Cache } = require('../src/cache')
const createStorage = require('../src/storage')

const { test } = t

test('dedupe only', async (t) => {
  test('should dedupe without using the cache storage', async (t) => {
    const cache = new Cache({
      ttl: 1,
      dedupeOnly: true,
      storage: createStorage()
    })

    let spy = 0
    cache.define('f', {
      serialize: ({ p1, p2 }) => `${p1}~${p2}`
    }, ({ p1, p2 }) => {
      for (let i = 0; i < 1_000; i++) {
        // sync block
      }
      spy++
      return p1 + p2
    })

    await Promise.all([
      cache.f({ p1: 1, p2: 2 }),
      cache.f({ p1: 1, p2: 2 }),
      cache.f({ p1: 1, p2: 2 }),
      cache.f({ p1: 1, p2: 2 })
    ])

    t.equal(spy, 1)
    t.equal(await cache.use('f', '1~2'), undefined)
  })

  test('should dedupe without using the cache storage with references', async (t) => {
    const cache = new Cache({
      ttl: 1,
      dedupeOnly: true,
      storage: createStorage()
    })

    let spy = 0
    cache.define('f', {
      serialize: ({ p1, p2 }) => `${p1}~${p2}`,
      references: async (args, key, result) => {
        t.fail('must not call references with dedupe only')
        return ['some-reference']
      }
    }, ({ p1, p2 }) => {
      for (let i = 0; i < 1_000; i++) {
        // sync block
      }
      spy++
      return p1 + p2
    })

    await Promise.all([
      cache.f({ p1: 1, p2: 2 }),
      cache.f({ p1: 1, p2: 2 }),
      cache.f({ p1: 1, p2: 2 }),
      cache.f({ p1: 1, p2: 2 })
    ])

    t.equal(spy, 1)
    t.equal(await cache.use('f', '1~2'), undefined)
  })
})
