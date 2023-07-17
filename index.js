const { EventEmitter } = require('events')
const amqplib = require('amqplib')
const logger = require('./logger')

// Set the default max listeners to 100
EventEmitter.defaultMaxListeners = 100

let connection = null
let channel = null

// Reconnect to rabbit after waiting 5 seconds
const reconnect = async options => {
  try {
    logger.info('Attempting to reconnect in 5 seconds...')
    setTimeout(() => connect(options), 5000)
  } catch (error) {
    logger.error('Rabbit.reconnect', error)
  }
}

// Retry the message a maximum of 5 times
const attempt = async (callback, retry = 0) => {
  try {
    await callback()
    return true
  } catch (error) {
    if (retry < 5) attempt(callback, retry + 1)
    else return false
  }
}

const connect = async options => {
  try {
    // Extract the exchange, queue, and credentials from the options
    const { exchange, queue, credentials } = options

    // Extract the connection credentials
    const { RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_PORT } =
      credentials

    // Get the rabbit connection, otherwise make one
    connection = connection
      ? connection
      : await amqplib.connect(
          `amqp://${RABBIT_USER}:${RABBIT_PASSWORD}@${RABBIT_HOST}:${RABBIT_PORT}/`
        )

    // Add event listeners to reconnect when the connection is lost
    connection.once('close', async () => {
      logger.error('Rabbit connection closed')
      reconnect(options)
    })

    connection.once('error', async error => {
      logger.error('Rabbit connection error', error)
      reconnect(options)
    })

    // Get the rabbit channel, otherwise make one
    channel = channel ? channel : await connection.createChannel()

    if (queue) {
      // Assert the dead letter exchange
      await channel.assertExchange('dlx_exchange', 'direct', { durable: true })

      // Assert the dead letter queue
      await channel.assertQueue('dlx_queue', { durable: true })

      // Bind the dead letter queue to the dead letter exchange
      await channel.bindQueue('dlx_queue', 'dlx_exchange', 'dlx_routing_key')

      // Assert the queue with the dead letter exchange
      await channel.assertQueue(queue, {
        durable: true,
        arguments: {
          'x-dead-letter-exchange': 'dlx_exchange',
          'x-dead-letter-routing-key': 'dlx_routing_key',
        },
      })

      logger.info(`Asserted queue ${queue}`)
    }

    if (exchange) {
      // Assert the exchange
      await channel.assertExchange(exchange, 'fanout', { durable: true })

      logger.info(`Asserted exchange ${exchange}`)
    }

    // If both the exchange and queue were specified then bind the queue to the exchange
    if (exchange && queue) {
      await channel.bindQueue(queue, exchange, '')

      logger.info(`Bound queue ${queue} to the ${exchange} exchange`)
    }
  } catch (error) {
    logger.error('Rabbit.connect', error)
  }
}

const send = async options => {
  try {
    // Extract the exchange, queue, type, and data from the options
    const { exchange, queue, type, data } = options

    // Legacy data structure to support message lib
    const legacyData = Buffer.from(
      JSON.stringify({
        ...data,
        source: exchange,
        eventType: type,
        data,
      })
    )

    // Publish the message to the queue
    if (queue) await channel.sendToQueue(queueName, legacyData)

    // Publish the message to the exchange
    if (exchange) channel.publish(exchange, '', legacyData, { type })
  } catch (error) {
    logger.error('Rabbit.send', error)
  }
}

const listen = async (queue, events) => {
  try {
    // Consume messages that are sent to the queue
    await channel.consume(queue, async message => {
      const { properties, content } = message
      const data = JSON.parse(content.toString())

      // Grab the event from the events object
      const event = events[properties.type] || events[data.eventType]

      // If the event doesn't exist then acknowledge it and end execution
      if (!event) return channel.ack(message)

      // Pass the parsed message data to the event callback
      const callback = () => event(data)

      // Attempt the callback 5 times before failing the message
      const success = await attempt(callback)

      // If the message succeeded then acknowledge it
      if (success) channel.ack(message)
      // If the message failed then send it to the dead letter exchange
      else channel.reject(message, false)
    })
  } catch (error) {
    logger.error('Rabbit.listen', error)
  }
}

module.exports = {
  connect,
  send,
  listen,
}
