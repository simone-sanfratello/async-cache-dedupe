'use strict'

const Redis = require('ioredis')

async function sync (options) {
  options = options ? JSON.parse(options) : {}

  console.log('>>> worker')
  // console.log({ options })

  const redisClient = new Redis({
    ...options,
    db: options.db || 0,
    connectionName: 'acd-event',
    readOnly: true
  })

  const db = options.db || 0

  // TODO try/catch

  const response = await redisClient.subscribe(`__keyevent@${db}__:expire`)
  console.log(`>>> subscribe response: ${response}`)

  redisClient.on('message', (channel, key) => {
    console.log(`>>> message: ${JSON.stringify({ channel, key })}`)
  })

  // @see https://redis.io/topics/notifications
  // redis-cli config set notify-keyspace-events KEA
  // redis-cli --csv psubscribe '__key*__:*'
}

// sync()

module.exports = sync
