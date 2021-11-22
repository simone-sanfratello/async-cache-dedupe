const hyperid = require('hyperid')

const CHANNEL = 'acd-storage-invaliation'

// TODO timeout: communication can fail, assume nodes goes down
// TODO interval: check for cleaner
// TODO on selected crash, select another one, then run "rebuildReferences"

// TODO test cases
// 1 node
// 1 node + 1 node
// 3 nodes start together
// N nodes, every node ends (randomly after X seconds)
// N nodes, every node crash (randomly after X seconds)

class Listener {

  constructor(options) {
    // TODO validate options
    this.log = options.log
    this.id = hyperid().id
    this.listener = options.listener
    this.db = options.db
    this.onExpired = options.onExpired
    this.onEvicted = options.onEvicted
    
    this.cluster = { nodes: new Map(), cleaner: null }
  }

  async start() {
    await this.subscribeCluster()
  }

  async end() {
    try {
      await this.unsubscribeCluster()
      await this.unsubscribeCleaner()
    } catch (err) {
      this.log.error({ msg: 'acd/storage/redis.listen error on redis unsubscribe', err })
      throw err
    }
  }

  async subscribeCluster() {
    if (this.subscribedCluster) {
      return
    }
    await this.subscribe(CHANNEL)
    this.listener.on('message', this.onMessage)
    this.subscribedCluster = true
    await this.send('connect')

    // check is the cleaner is already elected, or become the clenaer
    if (!this.cluster.cleaner) {
      // TODO get first node, if me, candidate as cleaner
      await this.send('!cleaner')
      // TODO wait for nodes ack (timeout)
    }
    // TODO setInterval(this.send('alive'), X)
  }

  async unsubscribeCluster() {
    if (!this.subscribedCluster) {
      return
    }
    await this.send('disconnect')
    await this.unsubscribe(CHANNEL)
    this.subscribedCluster = false
    // TODO clearInterval alive
  }

  // TODO listen also maxmemory for evicted keys
  // TODO check "notify-keyspace-events KEA" on redis if possible, or document
  // TODO document this
  // @see https://redis.io/topics/notifications
  // redis-cli config set notify-keyspace-events KEA
  async subscribeCleaner() {
    if (this.subscribedCleaner) {
      return
    }
    await this.subscribe(`__keyevent@${this.db}__:expired`)
    this.subscribedCleaner = true
  }

  async unsubscribeCleaner() {
    if (!this.subscribedCleaner) {
      return
    }
    await this.unsubscribe(`__keyevent@${this.db}__:expired`)
    this.subscribedCleaner = false
  }

  async subscribe(channel) {
    const subscribed = await this.listener.subscribe(channel)
    if (subscribed !== 1) {
      throw new Error('cant subscribe to redis')
    }
  }

  async unsubscribe(channel) {
    await this.listener.unsubscribe(channel)
    // TODO check result? throw?
  }

  async onMessage(channel, message) {
    this.log.debug({ msg: 'acd/storage/redis-listen.onMessage', channel, message })
    if (channel === CHANNEL) {
      this.receive(JSON.parse(message))
    } else if (channel === `__keyevent@${this.db}__:expired`) {
      this.onExpired(message)
    }
  }

  async send(content) {
    this.log.debug({ msg: 'acd/storage/redis-listen.send', content })
    // TODO if this.cluster.nodes.size < 1 return?
    const message = this.message(content)
    await this.listener.publish(CHANNEL, message)
    // TODO get at least one ack (timeout)
  }

  async receive(message) {
    this.log.debug({ msg: 'acd/storage/redis-listen.receive', message })
    switch(message.content) {
      case 'connect':
        this.cluster.nodes.add(message.node)
        // TODO send alive
        break
      case 'disconnect':
        this.cluster.nodes.delete(message.node)
        if(this.cluster.cleaner === message.node) {
          this.send('!cleaner')
        }
        break
      case '!cleaner':
        this.cluster.cleaner = message.node
        if(this.cluster.cleaner === this.id) {
          this.subscribeCleaner()
        } else {
          this.unsubscribeCleaner()
        }
        break
      case 'alive':
        // TODO if !this.cluster.nodes.get(message.node)
        // this.cluster.nodes.set(message.node)
        this.cluster.nodes.get(message.node).alive = Date.now() // TODO any better? hrtime?
        break
      }
  }

  async ack(message) {
    // TODO
  }

  async nack(message) {
    // TODO
  }

  message(content) {
    return JSON.stringify({ id: hyperid().id, node: this.id, content })
  }

}

module.exports = Listener

