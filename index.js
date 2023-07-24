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
    if (queue) await channel.sendToQueue(queue, legacyData)

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
      // Parse out the properties and data from the message
      const { properties, content } = message
      const { type, headers } = properties
      const data = JSON.parse(content.toString())
      const eventType = type || data.eventType

      // Grab the event from the events object
      const event = events[eventType]

      // If the event doesn't exist then acknowledge the message and end execution
      if (!event) return channel.ack(message)

      const callback = async (retry = 0) => {
        try {
          // Process the message
          await event({ ...data, headers })

          // If no error was thrown then acknowledge the message
          channel.ack(message)
        } catch (error) {
          logger.error(`Message type ${eventType} failed on attempt ${retry + 1} on the ${queue} queue at ${new Date()}`)
          
          // Attempt the event callback a maximum of 5 times
          if (retry < 5) callback(retry + 1)

          // Send the message to the dead letter queue with the original queue name in the headers
          else channel.reject(message, false, false, { queue })
        }
      }

      await callback()
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
