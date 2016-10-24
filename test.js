'use strict'

const test = require('tape')
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
      })
    })

    // no context
    bob.signAndSend({
      to: alice._recipientOpts,
      object: {
        [TYPE]: 'something',
        hey: 'ho'
      }
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

      carol.once('message', msg => {
        // if (msg.author !== alice.permalink) return

        t.same(msg.object.object, msg2wrapper.object)
        t.end()
        friends.forEach(f => f.destroy())
      })
    })
  })
})

function rethrow (err) {
  if (err) throw err
}
