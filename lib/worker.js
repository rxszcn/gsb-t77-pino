'use strict'

const EE = require('node:events')
const { pipeline, PassThrough } = require('node:stream')
const pino = require('../pino.js')
const build = require('pino-abstract-transport')
const loadTransportStreamBuilder = require('./transport-stream')

// This file is not checked by the code coverage tool,
// as it is not reliable.

/* istanbul ignore file */

/*
 * > Multiple targets & pipelines
 *
 *
 * ┌─────────────────────────────────────────────────┐    ┌─────┐
 * │                                                 │    │  p  │
 * │                                                 │    │  i  │
 * │                   target                        │    │  n  │
 * │               │ ────────────────────────────────┼────┤  o  │
 * │   targets     │   target                        │    │  .  │
 * │ ────────────► │ ────────────────────────────────┼────┤  m  │       source
 * │               │   target                        │    │  u  │         │
 * │               │ ────────────────────────────────┼────┤  l  │         │write
 * │               │                                 │    │  t  │         ▼
 * │               │  pipeline   ┌───────────────┐   │    │  i  │      ┌────────┐
 * │               │ ──────────► │  PassThrough  ├───┼────┤  s  ├──────┤        │
 * │               │             └───────────────┘   │    │  t  │ write│ Thread │
 * │               │                                 │    │  r  │◄─────┤ Stream │
 * │               │  pipeline   ┌───────────────┐   │    │  e  │      │        │
 * │               │ ──────────► │  PassThrough  ├───┼────┤  a  │      └────────┘
 * │                             └───────────────┘   │    │  m  │
 * │                                                 │    │     │
 * └─────────────────────────────────────────────────┘    └─────┘
 *
 *
 *
 *  > One single pipeline or target
 *
 *
 *                                                           source
 *                                                             │
 * ┌────────────────────────────────────────────────┐          │write
 * │                                                │          ▼
 * │                                                │      ┌────────┐
 * │   targets     │   target                       │      │        │
 * │ ────────────► │  ──────────────────────────────┤      │        │
 * │               │                                │      │        │
 * │                                                ├──────┤        │
 * │                                                │      │        │
 * │                                                │      │        │
 * │                     OR                         │      │        │
 * │                                                │      │        │
 * │                                                │      │        │
 * │                               ┌──────────────┐ │      │        │
 * │   targets     │   pipeline    │              │ │      │ Thread │
 * │ ────────────► │  ────────────►│ PassThrough  ├─┤      │ Stream │
 * │               │               │              │ │      │        │
 * │                               └──────────────┘ │      │        │
 * │                                                │      │        │
 * │                     OR                         │ write│        │
 * │                                                │◄─────┤        │
 * │                                                │      │        │
 * │                ┌──────────────┐                │      │        │
 * │    pipeline    │              │                │      │        │
 * │ ──────────────►│ PassThrough  ├────────────────┤      │        │
 * │                │              │                │      │        │
 * │                └──────────────┘                │      └────────┘
 * │                                                │
 * │                                                │
 * └────────────────────────────────────────────────┘
 */

module.exports = async function ({ targets, pipelines, levels, dedupe }) {
  const targetStreams = []

  // Process targets
  if (targets && targets.length) {
    targets = await Promise.all(targets.map(async (t) => {
      const fn = await loadTransportStreamBuilder(t.target)
      const stream = await fn(t.options)
      return {
        level: t.level,
        stream
      }
    }))

    targetStreams.push(...targets)
  }

  // Process pipelines
  if (pipelines && pipelines.length) {
    pipelines = await Promise.all(
      pipelines.map(async (p) => {
        let level
        const pipeDests = await Promise.all(
          p.map(async (t) => {
            // level assigned to pipeline is duplicated over all its targets, just store it
            level = t.level
            const fn = await loadTransportStreamBuilder(t.target)
            const stream = await fn(t.options)
            return stream
          }
          ))

        return {
          level,
          stream: createPipeline(pipeDests)
        }
      })
    )
    targetStreams.push(...pipelines)
  }

  // Skip building the multistream step only when one single pipeline or target
  // is defined and it carries no `level` option, and return directly the
  // stream instance back to ThreadStream.
  // This is equivalent to define either:
  //
  // pino.transport({ target: ... })
  //
  // OR
  //
  // pino.transport({ pipeline: ... })
  //
  // A single target (or pipeline) with a `level` option must go through the
  // very same multistream filtering as multiple targets, so that adding an
  // unrelated target never changes which lines an existing target receives.
  if (targetStreams.length === 1 && targetStreams[0].level === undefined) {
    return targetStreams[0].stream
  } else {
    return build(process, {
      parse: 'lines',
      parseLine: parseLevel,
      metadata: true,
      close (err, cb) {
        let expected = 0
        for (const transport of targetStreams) {
          expected++
          transport.stream.on('close', closeCb)
          transport.stream.end()
        }

        function closeCb () {
          if (--expected === 0) {
            cb(err)
          }
        }
      }
    })
  }

  // Extracts the log level from a serialized line without parsing the whole
  // line: pino always writes `level` as the first property of the JSON object
  // (`{"level":30,...`), so a cheap prefix scan covers the hot path.
  // Anything shaped differently (e.g. a custom `formatters.level`) falls back
  // to a full JSON.parse, matching the previous behavior for those lines.
  function parseLevel (line) {
    if (line.startsWith('{"level":')) {
      let level = 0
      let i = 9
      while (i < line.length) {
        const code = line.charCodeAt(i)
        if (code < 48 || code > 57) break
        level = level * 10 + (code - 48)
        i++
      }
      if (i > 9) {
        return { level }
      }
    }
    return JSON.parse(line)
  }

  // TODO: Why split2 was not used for pipelines?
  function process (stream) {
    const multi = pino.multistream(targetStreams, { levels, dedupe })
    // TODO manage backpressure
    stream.on('data', function (chunk) {
      const { lastTime, lastMsg, lastObj, lastLevel } = this
      multi.lastLevel = lastLevel
      multi.lastTime = lastTime
      multi.lastMsg = lastMsg
      multi.lastObj = lastObj

      // TODO handle backpressure
      multi.write(chunk + '\n')
    })
  }

  /**
 * Creates a pipeline using the provided streams and return an instance of `PassThrough` stream
 * as a source for the pipeline.
 *
 * @param {(TransformStream|WritableStream)[]} streams An array of streams.
 *   All intermediate streams in the array *MUST* be `Transform` streams and only the last one `Writable`.
 * @returns A `PassThrough` stream instance representing the source stream of the pipeline
 */
  function createPipeline (streams) {
    const ee = new EE()
    const stream = new PassThrough({
      autoDestroy: true,
      destroy (_, cb) {
        ee.on('error', cb)
        ee.on('closed', cb)
      }
    })

    pipeline(stream, ...streams, function (err) {
      if (err && err.code !== 'ERR_STREAM_PREMATURE_CLOSE') {
        ee.emit('error', err)
        return
      }

      ee.emit('closed')
    })

    return stream
  }
}
