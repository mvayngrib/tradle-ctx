'use strict'

/** @module contextsDB */

const EventEmitter = require('events').EventEmitter
const debug = require('debug')('tradle:dbs:context')
const once = require('once')
const deepEqual = require('deep-equal')
const pump = require('pump')
const through = require('through2')
const indexer = require('feed-indexer')
const lexint = require('lexicographic-integer')
const clone = require('xtend')
const parallel = require('run-parallel')
const PassThrough = require('readable-stream').PassThrough
const tradle = require('@tradle/engine')
const topics = tradle.topics
const types = tradle.types
const utils = tradle.utils
const typeforce = tradle.typeforce
const constants = tradle.constants
const SEQ = constants.SEQ
const TYPE = constants.TYPE
const MESSAGE_TYPE = constants.MESSAGE_TYPE
const ENTRY_PROP = constants.ENTRY_PROP

/**
 * @typedef {Object} contextsDB
 */

/**
 * share messages by context property
 *
 * @alias module:contextsDB
 * @param  {Object}   opts
 * @param  {Object}   opts.node            @tradle/engine node instance
 * @param  {Object}   opts.db              database to use to track message context
 * @param  {Function} [opts.getMessageSeq] calculate the seq of a message object (defaults to msg => msg[SEQ])
 * @param  {Function} [opts.getContext]    calculate the context of a message object (defaults to msg => msg.context)
 * @param  {Function} [opts.worker]        what to do with a given message (defaults to forwarding to shared-with party)
 */
module.exports = function createContextDB (opts) {
  typeforce({
    node: typeforce.Object,
    db: typeforce.String,
    // pass these in to override the defaults
    getMessageSeq: typeforce.maybe(typeforce.Function),
    getContext: typeforce.maybe(typeforce.Function),
    worker: typeforce.maybe(typeforce.Function)
  }, opts)

  const node = opts.node
  const keeper = node.keeper
  const customTopics = {
    sharecontext: 'sharectx',
    unsharecontext: 'unsharectx'
  }

  let closed
  const msgDB = node._createDB('msg-' + opts.db)
  msgDB.once('closing', () => closed = true)

  const ctxDB = node._createDB('ctx-' + opts.db)
  ctxDB.once('closing', () => closed = true)

  node.once('destroying', close)

  const getMessageSeq = opts.getMessageSeq || defaultGetMessageSeq
  const getContext = wrapGetContext(opts.getContext || defaultGetContext)
  const worker = opts.worker || defaultWorker
  const indexedMsgDB = indexer({
    feed: node.changes,
    db: msgDB,
    primaryKey: 'permalink',
    entryProp: ENTRY_PROP,
    preprocess: function (change, cb) {
      if (closed) return

      const val = change.value
      if (val.topic !== topics.newobj || val.type !== MESSAGE_TYPE) return cb()

      keeper.get(val.permalink, function (err, body) {
        if (err) return cb()

        val.object = body
        cb(null, change)
      })
    },
    filter: value => value.topic === topics.newobj && value.type === MESSAGE_TYPE,
    reduce: function (state, change, cb) {
      const val = change.value
      // each message's state gets written exactly once
      if (state) return cb(null, state)

      const context = state ? state.context : getContext(val)
      if (!context) return cb()

      return cb(null, {
        permalink: val.permalink,
        context: context,
        recipient: getRecipient(val),
        seq: getMessageSeq(change) // not the same as message.seq
      })
    }
  })

  const msgDBIndexes = {
    context: indexedMsgDB.by('context', function (state) {
      // order by seq
      return state.context + sep + hex(state.seq) + sep + state.permalink
    })
  }

  const indexedCtxDB = indexer({
    feed: node.changes,
    db: ctxDB,
    primaryKey: value => {
      const context = getContext(value, true)
      if (context) return `${context}:${getRecipient(value)}`
    },
    entryProp: ENTRY_PROP,
    preprocess: function (change, cb) {
      if (closed) return

      const val = change.value
      if (val.topic !== topics.newobj) return cb(null, change)

      keeper.get(val.permalink, function (err, body) {
        if (err) return cb()

        val.object = body
        if (body[TYPE] === MESSAGE_TYPE) {
          return node.objects.get(val.objectinfo.permalink, function (err, metadata) {
            if (err) return cb()

            val.objectinfo = metadata
            cb(null, change)
          })
        }

        cb(null, change)
      })
    },
    filter: function (val) {
      return (val.topic === topics.newobj && val.type === MESSAGE_TYPE) ||
            val.topic === customTopics.sharecontext ||
            val.topic === customTopics.unsharecontext
    },
    reduce: function (state, change, cb) {
      const val = change.value
      const context = state ? state.context : getContext(val, true)
      if (!context) return cb()

      let newState
      switch (val.topic) {
      case topics.newobj:
        newState = state ? clone(state) : newContextState({
          context: context,
          recipient: getRecipient(val)
        })

        if (val.object.object && val.object.object[TYPE] === MESSAGE_TYPE) {
          // get original msg
          return node.objects.get(val.objectinfo.link, function (err, originalMsg) {
            // TODO: figure out the right thing to do here in case of err
            if (err) return cb()

            newState.seq = getMessageSeq({ change: originalMsg[ENTRY_PROP], value: originalMsg })
            cb(null, newState)
          })
        }

        newState.seq = getMessageSeq(change)
        break
      case customTopics.sharecontext:
        newState = state ? clone(state) : newContextState(val)
        newState.active = true
        break
      case customTopics.unsharecontext:
        if (!state) return cb() // was never shared

        newState = clone(state)
        newState.active = false
        break
      }

      if (deepEqual(state, newState)) {
        return cb()
      }

      cb(null, newState)
    }
  })

  const myDebug = utils.subdebugger(debug, node.name || node.shortlink)
  const sep = indexedCtxDB.separator
  const indexes = {}
  indexes.contextForRecipient = indexedCtxDB.by('cfr', function (state) {
    if (!state.context || !state.active) return

    // end with a `sep` otherwise we won't be able to stream with:
    //   eq:  context + sep + recipient
    return state.context + sep + state.recipient + sep
  })

  const forwarding = {}
  pump(
    cursor(),
    through.obj(function (data, enc, cb) {
      forward(data)
      cb()
    })
  )

  return {
    close,
    share,
    unshare,
    cursor,
    seq: position,
    context: createContextStream,
    messages
  }

  function position ({ context, recipient }, cb) {
    if (!(context && recipient)) {
      throw new Error('expected "context" and "recipient"')
    }

    cb = once(cb)
    cursor({
      eq: context + sep + recipient,
      live: false
    })
    .once('data', data => cb(null, data.seq))
    .on('error', cb)
    .on('end', () => cb(new Error('context not shared with recipient')))
  }

  function cursor (opts) {
    opts = clone({
      old: true,
      live: true,
      keys: false
    }, opts || {})

    return pump(
      indexes.contextForRecipient.createReadStream(opts),
      through.obj(function (data, enc, cb) {
        if (!data.active) return cb()
        cb(null, data)
      })
    )
  }

  function messages (opts) {
    const stream = new PassThrough({ objectMode: true })
    position(opts, function (err, seq) {
      if (err) {
        stream.emit('error', err)
        return stream.end()
      }

      pump(
        createContextStream(clone(opts, { seq })),
        stream
      )
    })

    return stream
  }

  function share ({ context, recipient, seq=0 }, cb) {
    node.changes.append({
      topic: customTopics.sharecontext,
      timestamp: utils.now(),
      context: context,
      recipient: recipient,
      seq: seq
    }, cb)
  }

  function unshare ({ context, recipient }, cb) {
    node.changes.append({
      topic: customTopics.unsharecontext,
      timestamp: utils.now(),
      context: context,
      recipient: recipient
    }, cb)
  }

  function close (cb) {
    if (closed) return

    closed = true
    parallel([
      done => msgDB.close(done),
      done => ctxDB.close(done)
    ], cb)
  }

  function forward (data) {
    const identifier = data.context + data.recipient
    if (forwarding[identifier]) return

    forwarding[identifier] = pump(
      createContextStream(data),
      through.obj(function (msgMeta, enc, cb) {
        worker({
          context: data.context,
          recipient: data.recipient,
          link: msgMeta.permalink,
          permalink: msgMeta.permalink
        }, cb)
      })
    )
  }

  function createContextStream (opts) {
    if (typeof opts === 'string') opts = { context: opts }
    if (!opts.context) throw new Error('expected "context"')

    return msgDBIndexes.context.createReadStream(clone({
      gt: opts.context + sep + hex(opts.seq || 0),
      lt: opts.context + sep + '\xff',
      old: true,
      live: true,
      keys: false
    }, opts))
  }


  function defaultWorker (data, cb) {
    // messages are immutable so permalink === link
    node.send({
      link: data.permalink,
      to: { permalink: data.recipient }
    }, cb)
  }
}

function getRecipient (val) {
  return val.recipient
}

function newContextState ({ context, recipient }) {
  return {
    context: context,
    recipient: recipient,
    seq: 0
  }
}

function defaultGetContext (val) {
  return val.object.context
}

function wrapGetContext (getContext) {
  return function (value, testSecondTier) {
    if (value.topic !== topics.newobj) return value.context

    // if this is a forwarded message
    // we need update our cursor so we don't re-forward this next time
    const base = testSecondTier && value.objectinfo.type === MESSAGE_TYPE ? value.objectinfo : value
    return getContext(base)
  }
}

function defaultGetMessageSeq (change) {
  return change.change
}

function hex (n) {
  return lexint.pack(n, 'hex')
}
