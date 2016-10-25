'use strict'

/** @module contextsDB */

const EventEmitter = require('events').EventEmitter
const debug = require('debug')('tradle:dbs:context')
const pump = require('pump')
const through = require('through2')
const indexer = require('feed-indexer')
const lexint = require('lexicographic-integer')
const clone = require('xtend')
const parallel = require('run-parallel')
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
 * blockchain seals database, bootstrapped from log
 *
 * @alias module:contextsDB
 * @param  {Object}   opts
 * @param  {Object}   opts.node            @tradle/engine node instance
 * @param  {Object}   opts.db              database to use to track message context
 * @param  {Function} [opts.getMessageSeq] calculate the seq of a message object (defaults to msg => msg[SEQ])
 * @param  {Function} [opts.getContext]    calculate the context of a message object (defaults to msg => msg.context)
 */
module.exports = function createContextDB (opts) {
  typeforce({
    node: typeforce.Object,
    db: typeforce.String,
    // pass these in to override the defaults
    getMessageSeq: typeforce.maybe(typeforce.Function),
    getContext: typeforce.maybe(typeforce.Function)
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
  const getContext = opts.getContext || defaultGetContext
  const indexedMsgDB = indexer({
    feed: node.changes,
    db: msgDB,
    primaryKey: 'permalink',
    entryProp: ENTRY_PROP,
    preprocess: preprocess,
    reduce: function (state, change, cb) {
      const val = change.value
      const context = getContext(val)
      if (!context) return cb()

      // each message's state gets written exactly once
      if (state)return cb(null, state)

      let newState
      switch (val.topic) {
      case topics.newobj:
        return cb(null, {
          permalink: val.permalink,
          context: context,
          recipient: getRecipient(val),
          seq: getMessageSeq(change) // not the same as message.seq
        })
      }

      cb()
    }
  })

  const msgDBIndexes = {
    context: indexedMsgDB.by('context', function (state) {
      // order by seq
      return state.context + sep + lexint.pack(state.seq) + sep + state.permalink
    })
  }

  const indexedCtxDB = indexer({
    feed: node.changes,
    db: ctxDB,
    primaryKey: value => `${getContext(value)}:${getRecipient(value)}`,
    entryProp: ENTRY_PROP,
    filter: function (val) {
      return val.topic === customTopics.sharecontext || val.topic === customTopics.unsharecontext
    },
    reduce: function (state, change, cb) {
      const val = change.value
      const context = getContext(val)
      if (!context) return cb()

      let newState
      switch (val.topic) {
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

      cb(null, newState)
    }
  })

  const myDebug = utils.subdebugger(debug, node.name || node.shortlink)
  const sep = indexedCtxDB.separator
  const indexes = {}
  // indexes.recipient = indexedCtxDB.by('recipient')
  // indexes.active = indexedCtxDB.by('active')
  indexes.context = indexedCtxDB.by('context', function (state) {
    if (!state.context) return
    return state.context + sep + lexint.pack(state.seq)
  })

  indexes.contextForRecipient = indexedCtxDB.by('cfr', function (state) {
    if (!state.context) return
    return state.active + sep + state.context + sep + state.recipient // + sep + lexint.pack(state.seq)
  })

  const forwarding = {}
  pump(
    indexes.contextForRecipient.createReadStream({ live: true, keys: false }),
    through.obj(function (data, enc, cb) {
      forward(data)
      cb()
    })
  )

  return {
    close,
    share,
    unshare
  }

  function share ({ context, recipient, seq=0 }, cb) {
    node.changes.append({
      topic: 'sharectx',
      timestamp: utils.now(),
      context: context,
      recipient: recipient,
      seq: seq
    }, cb)
  }

  function unshare ({ context, recipient }, cb) {
    node.changes.append({
      topic: 'unsharectx',
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

    const to = { permalink: data.recipient }
    forwarding[identifier] = pump(
      getMessageStream(data),
      through.obj(function (data, enc, cb) {
        // messages are immutable so permalink === link
        node.send({ link: data.permalink, to: to }, cb)
      })
    )
  }

  function getMessageStream (data) {
    return msgDBIndexes.context.createReadStream({
      gte: data.context + sep + lexint.pack(data.seq),
      lte: data.context + sep + '\xff',
      old: true,
      live: true,
      keys: false
    })
  }

  function preprocess (change, cb) {
    if (closed) return

    const val = change.value
    const topic = val.topic
    if (val.type !== MESSAGE_TYPE || topic !== topics.newobj) return cb()

    keeper.get(val.permalink, function (err, body) {
      if (err) return cb(null, change)

      val.object = body
      cb(null, change)
    })
  }
}

function getRecipient (val) {
  return val.recipient
}

function newContextState (val) {
  return {
    context: val.context,
    recipient: val.recipient,
    seq: 0
  }
}

function defaultGetContext (val) {
  return val.context || val.object.context
}

function defaultGetMessageSeq (change) {
  return change.change
}
