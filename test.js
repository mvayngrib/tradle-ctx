'use strict'

const test = require('tape')
const collect = require('stream-collector')
const tradle = require('@tradle/engine')
const helpers = require('@tradle/engine/test/helpers')
const contexts = require('@tradle/engine/test/contexts')
const createContextsDB = require('./')
const constants = tradle.constants
const TYPE = constants.TYPE

test('contexts', function (t) {
  contexts.nFriends(3, function (err, friends) {
    if (err) throw err

    const contextDBs = friends.map(node => {
      return createContextsDB({
        node: node,
        db: 'contexts.db'
      })
    })

    const [alice, bob, carol] = friends
    // console.log('alice', alice.permalink)
    // console.log('bob', bob.permalink)
    // console.log('carol', carol.permalink)
    helpers.connect(friends)

    let link
    let context = 'boo!'
    const msg1 = {
      [TYPE]: 'something',
      hey: 'ho'
    }

    const msg2 = {
      [TYPE]: 'something else',
      hey: 'hey'
    }

    let msg1wrapper
    let msg2wrapper
    alice.signAndSend({
      to: bob._recipientOpts,
      object: msg1,
      other: {
        context: context
      }
    }, rethrow)

    bob.once('message', msg => {
      msg1wrapper = msg

      // bob
      // test share existing
      contextDBs[1].share({
        context,
        recipient: carol.permalink,
        seq: 0
      }, rethrow)

      collect(contextDBs[1].messages({
        context,
        recipient: carol.permalink,
        live: false
      }), function (err, msgs) {
        if (err) throw err

        t.equal(msgs.length, 1)
        t.equal(msgs[0].permalink, msg1wrapper.permalink)
      })
    })

    // no context
    bob.signAndSend({
      to: alice._recipientOpts,
      object: msg1
    }, rethrow)

    let togo = 2
    carol.once('message', msg => {
      // if (msg.author !== bob.permalink) return

      // msg = {
      //   ...metadata,
      //   object: {
      //     [TYPE]: 'tradle.Message',  // msg from bob
      //     object: {
      //       [TYPE]: 'tradle.Message' // original msg from alice
      //       object: {}               // msg1 body
      //     }
      //   }
      // }

      t.equal(msg.objectinfo.link, msg1wrapper.link)
      t.equal(msg.object.object.context, context)

      // test share live
      alice.signAndSend({
        to: bob._recipientOpts,
        object: msg2,
        other: {
          context: context
        }
      }, function (err, result) {
        if (err) throw err

        msg2wrapper = result.message
      })

      let received
      carol.on('message', msg => {
        t.notOk(received)
        t.same(msg.object.object, msg2wrapper.object)
        if (received) return // shouldn't happen, but let's prevent the loop

        received = true
        let cdb = contextDBs[1]
        cdb.close(function () {
          cdb = createContextsDB({
            node: friends[1],
            db: 'contexts.db'
          })

          // no messages should still be queued
          collect(cdb.messages({
            context: context,
            recipient: carol.permalink,
            live: false
          }), function (err, msgs) {
            if (err) throw err

            t.equal(msgs.length, 0)
            t.end()
            friends.forEach(friend => friend.destroy())
          })
        })
      })
    })
  })
})

test('custom context', function (t) {
  // alice and bob chat
  // bob sends dave some msgs
  // bob shares alice<=>bob conversation with carol

  contexts.nFriends(4, function (err, friends) {
    if (err) throw err

    const contextDBs = friends.map(node => {
      return createContextsDB({
        node: node,
        db: 'contexts.db',
        // context is a conversation
        getContext: val => {
          return getConversationIdentifier(val.author, val.recipient)
        }
      })
    })

    const [alice, bob, carol, dave] = friends
    helpers.connect(friends)

    const aliceBobConvo = []
    ;[
      {
        [TYPE]: 'something',
        hey: 'hey'
      },
      {
        [TYPE]: 'something else',
        hey: 'ho'
      }
    ].forEach(obj => {
      alice.signAndSend({
        to: bob._recipientOpts,
        object: obj
      }, function (err, result) {
        if (err) throw err

        aliceBobConvo.push(result.message.object)
      })

      bob.signAndSend({
        to: dave._recipientOpts,
        object: obj
      }, rethrow)
    })

    bob.on('message', function (msg) {
      bob.signAndSend({
        to: { permalink: msg.author },
        object: {
          [TYPE]: 'blah',
          not: 'around'
        }
      }, function (err, sent) {
        if (err) throw err

        aliceBobConvo.push(sent.message.object)
      })
    })

    // bob
    contextDBs[1].share({
      context: getConversationIdentifier(alice.permalink, bob.permalink),
      recipient: carol.permalink,
      seq: 0
    }, rethrow)

    carol.on('message', function (msg) {
      t.same(msg.object.object, aliceBobConvo.shift())
      if (aliceBobConvo.length) return

      t.end()
      friends.forEach(friend => friend.destroy())
    })
  })
})

function rethrow (err) {
  if (err) throw err
}

function alphabetical (a, b) {
  return a < b ? -1 : a === b ? 0 : 1
}

function getConversationIdentifier (a, b) {
  return [a, b].sort(alphabetical).join(':')
}
