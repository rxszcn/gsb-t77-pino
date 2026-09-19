// 在仓库根目录跑：node repro/transport-single-target-level.js
const { join } = require('node:path')
const fs = require('node:fs')
const os = require('node:os')
const pino = require('../pino.js')

const dir = fs.mkdtempSync(join(os.tmpdir(), 'gsb-pino-'))
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function logAll (logger) {
  logger.debug('d')
  logger.info('i')
  logger.warn('w')
  logger.error('e')
}

function observed (name) {
  let raw = ''
  try { raw = fs.readFileSync(join(dir, name), 'utf8') } catch (e) { return [] }
  return raw.split('\n').filter(Boolean).map((l) => {
    try { return JSON.parse(l).level } catch (e) { return '?' }
  })
}

function show (title, expect, got) {
  console.log(title)
  console.log('  应到: [' + expect.join(',') + ']   实到: [' + got.join(',') + ']   行数 ' + got.length)
}

async function runTransport (name, transportOptions) {
  const stream = pino.transport(transportOptions)
  const logger = pino({ level: 'debug' }, stream)
  logAll(logger)
  await sleep(600)
  logger.flush()
  await sleep(600)
  return observed(name)
}

async function main () {
  const ALL = [20, 30, 40, 50]
  const ONLY_ERROR = [50]
  const NO_DEBUG = [30, 40, 50]

  // A) targets 只有一个元素、且带 level
  const a = await runTransport('a.log', {
    targets: [{ target: 'pino/file', options: { destination: join(dir, 'a.log'), mkdir: true }, level: 'error' }]
  })
  show('A) targets:[{ level: "error" }]  单元素', ONLY_ERROR, a)

  // B) 同样这条配上第二个 target
  const b1 = await runTransport('b1.log', {
    targets: [
      { target: 'pino/file', options: { destination: join(dir, 'b1.log'), mkdir: true }, level: 'error' },
      { target: 'pino/file', options: { destination: join(dir, 'b2.log'), mkdir: true } }
    ]
  })
  const b2 = observed('b2.log')
  show('B) 第一条同上，加了第二条（不写 level）→ 第一路', ONLY_ERROR, b1)
  show('B) 第二路（没写 level，默认 info）', NO_DEBUG, b2)

  // C) 单目标写法，不带 level
  const c = await runTransport('c.log', {
    target: 'pino/file', options: { destination: join(dir, 'c.log'), mkdir: true }
  })
  show('C) target: 单目标（文档说不走多路通路）', ALL, c)

  // D) targets 单元素但不写 level
  const d = await runTransport('d.log', {
    targets: [{ target: 'pino/file', options: { destination: join(dir, 'd.log'), mkdir: true } }]
  })
  show('D) targets:[{ 无 level }] 单元素', ALL, d)

  // E) targets 单元素带 level，但主 logger 级别更低
  const e = await runTransport('e.log', {
    targets: [{ target: 'pino/file', options: { destination: join(dir, 'e.log'), mkdir: true }, level: 'warn' }]
  })
  show('E) targets:[{ level: "warn" }] 单元素', [40, 50], e)

  console.log('临时目录: ' + dir)
}

main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1) })
