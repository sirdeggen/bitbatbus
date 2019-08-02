const fs = require('fs-extra')
const recursive = require('recursive-readdir')
const crypto = require('crypto')
let fspath = './data'
let buspath = './bus'

const B = '19HxigV4QyBv3tHpQVcUEQyq1pzZVdoAut'
const BCAT = '15DHFxWZJT58f9nhyGnsRBqrgwK4W6h4Up'
const BCHUNK = '1ChDHzdd1H4wSjgGMHyndZm6qxEDGjqpJL'

async function hashFile (filename) {
  const sum = crypto.createHash('sha256')
  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(filename)
    fileStream.on('error', reject)
    fileStream.on('data', (chunk) => {
      try {
        sum.update(chunk)
      } catch (err) {
        reject(err)
      }
    })
    fileStream.on('end', () => {
      resolve(sum.digest('hex'))
    })
  })
}

async function createCLink (source) {
  let hash = await hashFile(source)
  const filePath = `${fspath}/c/${hash}`
  if (!await fs.pathExists(filePath)) {
    await fs.rename(source, filePath)
  } else {
    await fs.unlink(source)
  };
  await fs.link(filePath, source)

  return hash
}

async function saveB (txId, opRet) {
  if (await fs.pathExists(`${fspath}/b/${txId}`)) return

  const data = opRet.lb2 || opRet.b2 || ''
  if (typeof data !== 'string') return
  var buffer = Buffer.from(data, 'base64')

  // console.log('Saving B: ', txId)
  const bPath = `${fspath}/b/${txId}`
  await fs.writeFile(bPath, buffer)
  await createCLink(bPath)
}

async function saveChunk (txId, opRet) {
  const filepath = `${fspath}/chunks/${txId}`
  if (await fs.pathExists(filepath)) return

  // console.log(`Saving Chunk: ${txId}`)
  const data = opRet.lb2 || opRet.b2 || ''
  if (typeof data !== 'string') return
  var buffer = Buffer.from(data, 'base64')

  await fs.writeFile(filepath, buffer)
}

async function streamFile (destPath, bcat) {
  return new Promise((resolve, reject) => {
    const writer = fs.createWriteStream(destPath, { flags: 'a' })
      .on('error', reject)
      .on('close', resolve)
      .setMaxListeners(1000)
    bcat.chunks.forEach((chunkId) => {
      fs.createReadStream(`${fspath}/chunks/${chunkId}`)
        .on('error', reject)
        .pipe(writer)
    })
    writer.close()
  })
}

async function saveBCat (bcat) {
  const destPath = `${fspath}/newbcat/${bcat.txId}`
  if (!bcat.chunks.length || await fs.pathExists(destPath)) return
  for (let chunkId of bcat.chunks) {
    if (!await fs.pathExists(`${fspath}/chunks/${chunkId}`)) return
  }

  // console.log('Saving BCAT: ', bcat.txId)
  // stream chunks into one file
  await streamFile(destPath, bcat)

  await createCLink(destPath)
}

async function processTransaction (txn) {
  const opRet = txn.out.find((out) => out.b0.op === 106) // OP 118 could be the wrong way round
  try {
    console.log(`op ${txn.out[0].b0.op} found in outpu 0 ${txn.tx.h}`)
  } catch (e) {
    console.log(e)
  }
  try {
    console.log(`op ${txn.out[1].b0.op} found in output 1 ${txn.tx.h}`)
  } catch (e) {
    console.log(e)
  }
  if (!opRet) {
    console.log(`stopping with ${txn.tx.h}`)
    return
  }
  console.log(`continuing with ${txn.tx.h}`)
  let bcat
  try {
    switch (opRet.s1) {
      case B:
        // console.log(`Processing B: ${txn.tx.h}`)
        return saveB(txn.tx.h, opRet)

      case BCAT:
        // console.log(`Processing BCAT: ${txn.tx.h}`)
        bcat = {
          txId: txn.tx.h,
          chunks: [],
          fileData: {
            info: opRet.s2,
            contentType: opRet.s3,
            encoding: opRet.s4,
            filename: opRet.s5
          }
        }

        let i = 7
        let chunkId
        while (opRet[`h${i}`]) {
          chunkId = opRet[`h${i}`]
          // console.log(`adding chunk h${i}`)
          bcat.chunks.push(chunkId)
          i++
        }

        // console.log(bcat)
        return saveBCat(bcat)

      case BCHUNK:
        // console.log(`Processing Chunk: ${txn.tx.h}`)
        await saveChunk(txn.tx.h, opRet)
        break

      default:
        console.log('no op_return')
        return
    }
  } catch (e) {
    console.log(e)
  }
}

const start = 593100
const finish = 593600
var height = 0

fs.ensureDir(fspath + '/chunks')
fs.ensureDir(fspath + '/c')
fs.ensureDir(fspath + '/b')
fs.ensureDir(fspath + '/newbcat')

// take a look at a block in the os, and for each:
recursive(buspath, (err, files) => {
  // handling error
  if (err) {
    return console.log('Unable to scan directory: ' + err)
  }
  // listing all files using forEach
  files.forEach((dir) => {
    var file = dir.split('/')[2]
    if (file.endsWith('.json') && file !== 'mempool.json') {
      height = Number(file.split('.')[0])
      if (finish >= height && height >= start) {
        try {
          fs.readJson(dir, (err, blockFile) => {
            // console.log(`Processing Block ${file}\n`)
            processBlock(blockFile)
            if (err) return console.log(err)
          })
        } catch (err) {
          console.log(err)
        }
      }
    }
  })
})

async function processBlock (block) {
  try {
    for (let tx of block) {
      await processTransaction(tx)
    }
  } catch (err) {
    console.log(err)
  }
}
