const { topic = 'email', kafkaHost = 'kafka-hs', kafka = true } = require('minimist')(process.argv.slice(2))
const { json, send } = require('micro')
const { Consumer, KafkaClient } = require('kafka-node')

const fs = require('fs')
const util = require('util')
const aws = require('aws-sdk')
const nodemailer = require('nodemailer')
const marked = require('marked')
const handlebars = require('handlebars')

aws.config.update({ region: 'us-east-1' })
const transporter = nodemailer.createTransport({
  SES: new aws.SES({
    apiVersion: '2010-12-01'
  })
})

const sendMail = (a) => new Promise((resolve, reject) => {
  transporter.sendMail(a, (err, info) => err ? reject(err) : resolve(info))
})
const readFile = util.promisify(fs.readFile)

if (kafka) {
  console.log('Kafka Init')
  const client = new KafkaClient({ kafkaHost })
  const consumer = new Consumer(client, [{ topic }], {})

  consumer.on('message', async msg => {
    const email = await prepareEmail(msg)
    sendMail(email)
  })
} else {
  console.log('Kafka Init Skipped')
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') return send(res, 404, { error: 'Not Found' })
  const email = await prepareEmail(await json(req))
  await sendMail(email)
  send(res, 200, { success: true })
}

async function prepareEmail (email) {
  let { body, template } = email
  delete email.body
  delete email.template
  const templateFn = await getTemplate(template)
  email.html = templateFn(body)
  return email
}

async function getTemplate (name = 'default') {
  try {
    const md = await readFile(`${__dirname}/templates/${name}.md`, 'utf8')
    const fn = handlebars.compile(md)
    return (data) => marked(fn(data))
  } catch (e) {
    return d => d
    // return (data) => `Error compiling template: ${e.stack}\n\n${JSON.stringify(data)}`
  }
}
