// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ldb

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
  "path/filepath"

	"github.com/conformal/goleveldb/leveldb"
	"github.com/conformal/goleveldb/leveldb/cache"
	"github.com/conformal/goleveldb/leveldb/opt"
	"github.com/hlandauf/btcdb"
	"github.com/hlandauf/btcutil"
	"github.com/hlandauf/btcwire"
	"github.com/hlandauf/btcnet"
	"github.com/hlandauf/btcscript"
  "github.com/hlandauf/btcnamedb"
  "github.com/hlandau/xlog"
  _ "github.com/hlandauf/btcnamedb/nameldb"
)

const (
	dbVersion     int = 2
	dbMaxTransCnt     = 20000
	dbMaxTransMem     = 64 * 1024 * 1024 // 64 MB
)

var log, Log = xlog.New("btc.db.ldb")

type tTxInsertData struct {
	txsha   *btcwire.ShaHash
	blockid int64
	txoff   int
	txlen   int
	usedbuf []byte
}

// LevelDb holds internal state for databse.
type LevelDb struct {
	// lock preventing multiple entry
	dbLock sync.Mutex

	// leveldb pieces
	lDb *leveldb.DB
	ro  *opt.ReadOptions
	wo  *opt.WriteOptions

	lbatch *leveldb.Batch

	nextBlock int64

	lastBlkShaCached bool
	lastBlkSha       btcwire.ShaHash
	lastBlkIdx       int64

	txUpdateMap      map[btcwire.ShaHash]*txUpdateObj
	txSpentUpdateMap map[btcwire.ShaHash]*spentTxUpdate

  nameDB btcnamedb.DB
}

var self = btcdb.DriverDB{DbType: "leveldb", CreateDB: CreateDB, OpenDB: OpenDB}

func init() {
	btcdb.AddDBDriver(self)
}

// parseArgs parses the arguments from the btcdb Open/Create methods.
func parseArgs(funcName string, args ...interface{}) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("Invalid arguments to ldb.%s -- "+
			"expected database path string", funcName)
	}
	dbPath, ok := args[0].(string)
	if !ok {
		return "", fmt.Errorf("First argument to ldb.%s is invalid -- "+
			"expected database path string", funcName)
	}
	return dbPath, nil
}

// OpenDB opens an existing database for use.
func OpenDB(args ...interface{}) (btcdb.Db, error) {
	dbpath, err := parseArgs("OpenDB", args...)
	if err != nil {
		return nil, err
	}

	db, err := openDB(dbpath, false)
	if err != nil {
		return nil, err
	}

	// Need to find last block and tx

	var lastknownblock, nextunknownblock, testblock int64

	increment := int64(100000)
	ldb := db.(*LevelDb)

	var lastSha *btcwire.ShaHash
	// forward scan
blockforward:
	for {

		sha, err := ldb.fetchBlockShaByHeight(testblock)
		if err == nil {
			// block is found
			lastSha = sha
			lastknownblock = testblock
			testblock += increment
		} else {
			if testblock == 0 {
				//no blocks in db, odd but ok.
				lastknownblock = -1
				nextunknownblock = 0
				var emptysha btcwire.ShaHash
				lastSha = &emptysha
			} else {
				nextunknownblock = testblock
			}
			break blockforward
		}
	}

	// narrow search
blocknarrow:
	for lastknownblock != -1 {
		testblock = (lastknownblock + nextunknownblock) / 2
		sha, err := ldb.fetchBlockShaByHeight(testblock)
		if err == nil {
			lastknownblock = testblock
			lastSha = sha
		} else {
			nextunknownblock = testblock
		}
		if lastknownblock+1 == nextunknownblock {
			break blocknarrow
		}
	}

	ldb.lastBlkSha = *lastSha
	ldb.lastBlkIdx = lastknownblock
	ldb.nextBlock = lastknownblock + 1

	return db, nil
}

// CurrentDBVersion is the database version.
var CurrentDBVersion int32 = 1

func openDB(dbpath string, create bool) (pbdb btcdb.Db, err error) {
	var db LevelDb
	var tlDb *leveldb.DB
  var nameDB btcnamedb.DB
	var dbversion int32

	defer func() {
		if err == nil {
			db.lDb = tlDb

			db.txUpdateMap = map[btcwire.ShaHash]*txUpdateObj{}
			db.txSpentUpdateMap = make(map[btcwire.ShaHash]*spentTxUpdate)

      db.nameDB = nameDB

			pbdb = &db
		}
	}()

	if create == true {
		err = os.Mkdir(dbpath, 0750)
		if err != nil {
			log.Errorf("mkdir failed %v %v", dbpath, err)
			return
		}
	} else {
		_, err = os.Stat(dbpath)
		if err != nil {
			err = btcdb.ErrDbDoesNotExist
			return
		}
	}

	needVersionFile := false
	verfile := dbpath + ".ver"
	fi, ferr := os.Open(verfile)
	if ferr == nil {
		defer fi.Close()

		ferr = binary.Read(fi, binary.LittleEndian, &dbversion)
		if ferr != nil {
			dbversion = ^0
		}
	} else {
		if create == true {
			needVersionFile = true
			dbversion = CurrentDBVersion
		}
	}

	myCache := cache.NewEmptyCache()
	opts := &opt.Options{
		BlockCache:   myCache,
		MaxOpenFiles: 256,
		Compression:  opt.NoCompression,
	}

	switch dbversion {
	case 0:
		opts = &opt.Options{}
	case 1:
		// uses defaults from above
	default:
		err = fmt.Errorf("unsupported db version %v", dbversion)
		return
	}

	tlDb, err = leveldb.OpenFile(dbpath, opts)
	if err != nil {
		return
	}

	// If we opened the database successfully on 'create'
	// update the
	if needVersionFile {
		fo, ferr := os.Create(verfile)
		if ferr != nil {
			// TODO(design) close and delete database?
			err = ferr
			return
		}
		defer fo.Close()
		err = binary.Write(fo, binary.LittleEndian, dbversion)
		if err != nil {
			return
		}
	}

  if create {
    nameDB, err = btcnamedb.CreateDB("leveldb", filepath.Join(dbpath,"../names_leveldb"))
  } else {
    nameDB, err = btcnamedb.OpenDB("leveldb", filepath.Join(dbpath,"../names_leveldb"))
  }

	return
}

// CreateDB creates, initializes and opens a database for use.
func CreateDB(args ...interface{}) (btcdb.Db, error) {
	dbpath, err := parseArgs("Create", args...)
	if err != nil {
		return nil, err
	}

	// No special setup needed, just OpenBB
	db, err := openDB(dbpath, true)
	if err == nil {
		ldb := db.(*LevelDb)
		ldb.lastBlkIdx = -1
		ldb.nextBlock = 0
	}
	return db, err
}

func (db *LevelDb) close() error {
  db.nameDB.Close() // XXX: error handling

	return db.lDb.Close()
}

// Sync verifies that the database is coherent on disk,
// and no outstanding transactions are in flight.
func (db *LevelDb) Sync() error {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	// while specified by the API, does nothing
	// however does grab lock to verify it does not return until other operations are complete.
	return nil
}

// Close cleanly shuts down database, syncing all data.
func (db *LevelDb) Close() error {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	return db.close()
}

// DropAfterBlockBySha will remove any blocks from the database after
// the given block.
func (db *LevelDb) DropAfterBlockBySha(sha *btcwire.ShaHash) (rerr error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()
	defer func() {
		if rerr == nil {
			rerr = db.processBatches()
		} else {
			db.lBatch().Reset()
		}
	}()

	startheight := db.nextBlock - 1

	keepidx, err := db.getBlkLoc(sha)
	if err != nil {
		// should the error here be normalized ?
		log.Tracef("block loc failed %v ", sha)
		return err
	}

	for height := startheight; height > keepidx; height = height - 1 {
		var blk *btcutil.Block
		blksha, buf, err := db.getBlkByHeight(height)
		if err != nil {
			return err
		}
		blk, err = btcutil.NewBlockFromBytes(buf)
		if err != nil {
			return err
		}

    err = db.undoNames(height)
    if err != nil {
      log.Warnf("Cannot undo names at height %d: %v", height, err)
      return err
    }

		for _, tx := range blk.MsgBlock().Transactions {
			err = db.unSpend(tx)
			if err != nil {
				return err
			}
		}
		// rather than iterate the list of tx backward, do it twice.
		for _, tx := range blk.Transactions() {
			var txUo txUpdateObj
			txUo.delete = true
			db.txUpdateMap[*tx.Sha()] = &txUo
		}
		db.lBatch().Delete(shaBlkToKey(blksha))
		db.lBatch().Delete(int64ToKey(height))

    _, err = db.expireNames(height+1, true)
    if err != nil {
      log.Warnf("Cannot unexpire names at height %d: %v", height, err)
      return err
    }
	}

	db.nextBlock = keepidx + 1

	return nil
}

// InsertBlock inserts raw block and transaction data from a block into the
// database.  The first block inserted into the database will be treated as the
// genesis block.  Every subsequent block insert requires the referenced parent
// block to already exist.
func (db *LevelDb) InsertBlock(block *btcutil.Block) (height int64, err error) {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()
	defer func() {
		if err == nil {
			err = db.processBatches()
		} else {
			db.lBatch().Reset()
		}
	}()

	blocksha, err := block.Sha()
	if err != nil {
		log.Warnf("Failed to compute block sha %v", blocksha)
		return 0, err
	}
	mblock := block.MsgBlock()
	rawMsg, err := block.Bytes()
	if err != nil {
		log.Warnf("Failed to obtain raw block sha %v", blocksha)
		return 0, err
	}
	txloc, err := block.TxLoc()
	if err != nil {
		log.Warnf("Failed to obtain raw block sha %v", blocksha)
		return 0, err
	}

	// Insert block into database
	newheight, err := db.insertBlockData(blocksha, &mblock.Header.PrevBlock,
		rawMsg)
	if err != nil {
		log.Warnf("Failed to insert block %v %v %v", blocksha,
			&mblock.Header.PrevBlock, err)
		return 0, err
	}

	// At least two blocks in the long past were generated by faulty
	// miners, the sha of the transaction exists in a previous block,
	// detect this condition and 'accept' the block.
	for txidx, tx := range mblock.Transactions {
		txsha, err := block.TxSha(txidx)
		if err != nil {
			log.Warnf("failed to compute tx name block %v idx %v err %v", blocksha, txidx, err)
			return 0, err
		}
		spentbuflen := (len(tx.TxOut) + 7) / 8
		spentbuf := make([]byte, spentbuflen, spentbuflen)
		if len(tx.TxOut)%8 != 0 {
			for i := uint(len(tx.TxOut) % 8); i < 8; i++ {
				spentbuf[spentbuflen-1] |= (byte(1) << i)
			}
		}

		err = db.insertTx(txsha, newheight, txloc[txidx].TxStart, txloc[txidx].TxLen, spentbuf)
		if err != nil {
			log.Warnf("block %v idx %v failed to insert tx %v %v err %v", blocksha, newheight, &txsha, txidx, err)
			return 0, err
		}

		// Some old blocks contain duplicate transactions
		// Attempt to cleanly bypass this problem by marking the
		// first as fully spent.
		// http://blockexplorer.com/b/91812 dup in 91842
		// http://blockexplorer.com/b/91722 dup in 91880
    if bugType, _ := btcnet.IsHistoricBug(newheight, txsha); bugType == btcnet.BugBitcoin {
      err = db.bugSpendOutput(txsha, 0)

      if err != nil {
        log.Warnf("block %v idx %v failed to spend tx %v %v err %v", blocksha, newheight, &txsha, txidx, err)
      }
    }

    err = db.applyNameTransaction(tx, txsha, newheight)
    if err != nil {
      log.Warnf("couldn't apply name tx: %v", err)
      return 0, err
    }

    _, err = db.expireNames(newheight+1, false)
    if err != nil {
      log.Warnf("couldn't expire names: %v", err)
      return 0, err
    }

		err = db.doSpend(tx)
		if err != nil {
			log.Warnf("block %v idx %v failed to spend tx %v %v err %v", blocksha, newheight, txsha, txidx, err)
			return 0, err
		}
	}
	return newheight, nil
}

func (db *LevelDb) applyNameTransaction(tx *btcwire.MsgTx, txsha *btcwire.ShaHash, txHeight int64) error {
  bugType, isBug := btcnet.IsHistoricBug(txHeight, txsha)
  if isBug && bugType != btcnet.BugBitcoin && bugType != btcnet.BugFullyApply {
    // namecore:names.cpp:ApplyNameTransaction
    if bugType == btcnet.BugFullyIgnore {
      for i, txOut := range tx.TxOut {
        ns, err := btcscript.NewNameScriptFromPk(txOut.PkScript)
        if err == nil && ns.IsAnyUpdate() {
          err = db.bugSpendOutput(txsha, uint32(i))
          if err != nil {
            return fmt.Errorf("Cannot spend buggy name output: %v", err)
          }
        }
      }
    }

    return nil
  }

  if !tx.IsNamecoin() {
    return nil
  }

  for i, txOut := range tx.TxOut {
    ns, err := btcscript.NewNameScriptFromPk(txOut.PkScript)
    if err == nil && ns.IsAnyUpdate() {
      name  := ns.OpName()
      value := ns.OpValue()

      nameInfo := btcwire.NameInfo{
        Key:       name,
        Value:     value,
        Height:    txHeight,
        OutPoint:  btcwire.NewOutPoint(txsha, uint32(i)),
        Addr:      txOut.PkScript,
      }

      err = db.nameDB.Set(nameInfo)
      if err != nil {
        return err
      }
    } else if err != nil {
      //log.Warnf("Namecoin transaction script parse error: %v", err)
    }
  }

  return nil
}

func (db *LevelDb) expireNames(height int64, unexpire bool) (names []string, err error) {
  // The genesis block contains no name expirations.
  if height == 0 {
    return
  }

  // Find out at which update heights names have expired since
  // the last block. If the expiration depth changes, this could
  // be multiple names at once.
  expDepthOld := btcwire.NameExpirationDepth(height-1)
  expDepthNow := btcwire.NameExpirationDepth(height)

  if expDepthNow > height {
    return
  }

  // Both are exclusive. The last expireTo was height-1-expDepthOld,
  // now we start at this value+1.
  expireFrom := height - expDepthOld
  expireTo   := height - expDepthNow

  // It is possible that expireFrom = expireTo+1, in case that the
  // expiration period is raised together with the block height. In
  // this case, no names expire in the current step. This case means
  // that the absolute expiration height "n - expirationDepth(n)" is
  // flat -- which is fine.
  if expireFrom > (expireTo+1) {
    panic("unexpected")
  }

  // Find all names that expire at those depths.
  expiringNames := map[string]struct{}{}

  for h := expireFrom; h <= expireTo; h++ {
    xnames, err := db.nameDB.GetNamesAtHeight(h)
    if err != nil {
      return nil, fmt.Errorf("expiry: couldn't get names at height: %d: %v", h, err)
    }

    for _, n := range xnames {
      expiringNames[n] = struct{}{}
    }
  }

  // Expire all those names.
  for name := range expiringNames {
    ni, err := db.nameDB.GetByKey(name)
    if err != nil {
      return nil, fmt.Errorf("expiry: couldn't get expiring key (%d): %v: %v", height, name, err)
    }

    if !ni.IsExpired(height) {
      return nil, fmt.Errorf("name is not actually expired: %s: height=%d, oldHeight=%d", name, height, ni.Height)
    }

    // Special rule: When d/postmortem expires (the name used by libcoin in the
    // name-stealing demonstration), its coin is already spent. Ignore.
    if height == 175868 && name == "d/postmortem" {
      continue
    }

    if unexpire {
      unspendErr := db.clearSpentData(&ni.OutPoint.Hash, ni.OutPoint.Index)
      if unspendErr != nil {
        log.Warnf("unexpiry: couldn't unspend output of de-expired name: %v: %v", name, unspendErr)
      }
    } else {
      spendErr := db.bugSpendOutput(&ni.OutPoint.Hash, ni.OutPoint.Index)
      if spendErr != nil {
        log.Warnf("expiry: couldn't spend output of expired name: %v: %v", name, spendErr)
      }
    }
  }

  for k := range expiringNames {
    names = append(names, k)
  }

  return
}

func (db *LevelDb) undoNames(height int64) error {
  xnames, err := db.nameDB.GetNamesAtHeight(height)
  if err != nil {
    return err
  }

  for _, name := range xnames {
    ni, err := db.nameDB.GetByKey(name)
    if err != nil {
      return fmt.Errorf("name undo: couldn't get name being undone at height %d: %v: %v", height, name, err)
    }

    if ni.Height != height {
      return fmt.Errorf("name undo: current name in database is not at height being undone: %v: %v != %v", name, ni.Height, height)
    }

    // Get the transaction via its out point so that we can find the input.
    tx, _, _, _, err := db.fetchTxDataBySha(&ni.OutPoint.Hash) // unspent txs
    if err != nil {
      return err
    }

    // Find the transaction which was the input to the current transaction,
    // the "n-1" transaction.
    var nmcPrevTx *btcwire.MsgTx
    var nmcPrevTxBlockHeight int64
    var nmcPrevTxSha *btcwire.ShaHash
    var nmcPrevNS *btcscript.NameScript
    var nmcPrevTxOutputIndex uint32
    var nmcPrevPkScript []byte
    for _, txIn := range tx.TxIn {
      // XXX: retrieves last spent tx, ignores other txs with same txid
      prevTx, err := db.fetchLastSpentTx(&txIn.PreviousOutPoint.Hash)
      if err != nil {
        return err
      }

      prevTxOut := prevTx.Tx.TxOut[txIn.PreviousOutPoint.Index]
      prevNS, err := btcscript.NewNameScriptFromPk(prevTxOut.PkScript)
      if err == nil { // is name op
        nmcPrevTx = prevTx.Tx
        nmcPrevTxBlockHeight = prevTx.Height
        nmcPrevNS = prevNS
        nmcPrevTxOutputIndex = txIn.PreviousOutPoint.Index
        nmcPrevPkScript = prevTxOut.PkScript
        nmcPrevTxSha = prevTx.Sha
        break
      }
    }

    if nmcPrevTx == nil {
      // no preceding namecoin transaction found, this shouldn't be possible.
      return fmt.Errorf("no previous namecoin transaction found while undoing name: %v: %v", name, height)
    }

    // If that transaction is a firstupdate or update transaction, set the
    // name data accordingly. Otherwise delete the name.
    if nmcPrevNS.IsAnyUpdate() {
      ni := btcwire.NameInfo{
        Key: name,
        Value: nmcPrevNS.OpValue(),
        Height: nmcPrevTxBlockHeight,
        OutPoint: btcwire.NewOutPoint(nmcPrevTxSha, nmcPrevTxOutputIndex),
        Addr: nmcPrevPkScript,
      }
      err = db.NameDB().Set(ni)
      if err != nil {
        return err
      }
    } else {
      err = db.NameDB().DeleteName(name)
      if err != nil {
        return err
      }
    }
  }

  // Delete the name-at-height entries and scrub the history.
  err = db.NameDB().DropAtHeight(height)
  if err != nil {
    return err
  }

  return nil
}

func (db *LevelDb) bugSpendOutput(txsha *btcwire.ShaHash, outputIdx uint32) error {
  po  := btcwire.NewOutPoint(txsha, uint32(outputIdx))
  txI := btcwire.NewTxIn(po, []byte("garbage"))

  var spendtx btcwire.MsgTx
  spendtx.AddTxIn(txI)
  return db.doSpend(&spendtx)
}

// doSpend iterates all TxIn in a bitcoin transaction marking each associated
// TxOut as spent.
func (db *LevelDb) doSpend(tx *btcwire.MsgTx) error {
	for txinidx := range tx.TxIn {
		txin := tx.TxIn[txinidx]

		inTxSha := txin.PreviousOutPoint.Hash
		inTxidx := txin.PreviousOutPoint.Index

		if inTxidx == ^uint32(0) {
			continue
		}

		//log.Infof("spending %v %v",  &inTxSha, inTxidx)

		err := db.setSpentData(&inTxSha, inTxidx)
		if err != nil {
			return err
		}
	}
	return nil
}

// unSpend iterates all TxIn in a bitcoin transaction marking each associated
// TxOut as unspent.
func (db *LevelDb) unSpend(tx *btcwire.MsgTx) error {
	for txinidx := range tx.TxIn {
		txin := tx.TxIn[txinidx]

		inTxSha := txin.PreviousOutPoint.Hash
		inTxidx := txin.PreviousOutPoint.Index

		if inTxidx == ^uint32(0) {
			continue
		}

		err := db.clearSpentData(&inTxSha, inTxidx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *LevelDb) setSpentData(sha *btcwire.ShaHash, idx uint32) error {
	return db.setclearSpentData(sha, idx, true)
}

func (db *LevelDb) clearSpentData(sha *btcwire.ShaHash, idx uint32) error {
	return db.setclearSpentData(sha, idx, false)
}

func (db *LevelDb) setclearSpentData(txsha *btcwire.ShaHash, idx uint32, set bool) error {
	var txUo *txUpdateObj
	var ok bool

	if txUo, ok = db.txUpdateMap[*txsha]; !ok {
		// not cached, load from db
		var txU txUpdateObj
		blkHeight, txOff, txLen, spentData, err := db.getTxData(txsha)
		if err != nil {
			// setting a fully spent tx is an error.
			if set == true {
				return err
			}
			// if we are clearing a tx and it wasn't found
			// in the tx table, it could be in the fully spent
			// (duplicates) table.
			spentTxList, err := db.getTxFullySpent(txsha)
			if err != nil {
				return err
			}

			// need to reslice the list to exclude the most recent.
			sTx := spentTxList[len(spentTxList)-1]
			spentTxList[len(spentTxList)-1] = nil
			if len(spentTxList) == 1 {
				// write entry to delete tx from spent pool
				// XXX
			} else {
				spentTxList = spentTxList[:len(spentTxList)-1]
				// XXX format sTxList and set update Table
			}

			// Create 'new' Tx update data.
			blkHeight = sTx.blkHeight
			txOff = sTx.txoff
			txLen = sTx.txlen
			spentbuflen := (sTx.numTxO + 7) / 8
			spentData = make([]byte, spentbuflen, spentbuflen)
			for i := range spentData {
				spentData[i] = ^byte(0)
			}
		}

		txU.txSha = txsha
		txU.blkHeight = blkHeight
		txU.txoff = txOff
		txU.txlen = txLen
		txU.spentData = spentData

		txUo = &txU
	}

	byteidx := idx / 8
	byteoff := idx % 8

	if set {
		txUo.spentData[byteidx] |= (byte(1) << byteoff)
	} else {
		txUo.spentData[byteidx] &= ^(byte(1) << byteoff)
	}

	// check for fully spent Tx
	fullySpent := true
	for _, val := range txUo.spentData {
		if val != ^byte(0) {
			fullySpent = false
			break
		}
	}
	if fullySpent {
		var txSu *spentTxUpdate
		// Look up Tx in fully spent table
		if txSuOld, ok := db.txSpentUpdateMap[*txsha]; ok {
			txSu = txSuOld
		} else {
			var txSuStore spentTxUpdate
			txSu = &txSuStore

			txSuOld, err := db.getTxFullySpent(txsha)
			if err == nil {
				txSu.txl = txSuOld
			}
		}

		// Fill in spentTx
		var sTx spentTx
		sTx.blkHeight = txUo.blkHeight
		sTx.txoff = txUo.txoff
		sTx.txlen = txUo.txlen
		// XXX -- there is no way to comput the real TxOut
		// from the spent array.
		sTx.numTxO = 8 * len(txUo.spentData)

		// append this txdata to fully spent txlist
		txSu.txl = append(txSu.txl, &sTx)

		// mark txsha as deleted in the txUpdateMap
		log.Tracef("***tx %v is fully spent\n", txsha)

		db.txSpentUpdateMap[*txsha] = txSu

		txUo.delete = true
		db.txUpdateMap[*txsha] = txUo
	} else {
		db.txUpdateMap[*txsha] = txUo
	}

	return nil
}

func int64ToKey(keyint int64) []byte {
	key := strconv.FormatInt(keyint, 10)
	return []byte(key)
}

func shaBlkToKey(sha *btcwire.ShaHash) []byte {
	shaB := sha.Bytes()
	return shaB
}

func shaTxToKey(sha *btcwire.ShaHash) []byte {
	shaB := sha.Bytes()
	shaB = append(shaB, "tx"...)
	return shaB
}

func shaSpentTxToKey(sha *btcwire.ShaHash) []byte {
	shaB := sha.Bytes()
	shaB = append(shaB, "sx"...)
	return shaB
}

func (db *LevelDb) lBatch() *leveldb.Batch {
	if db.lbatch == nil {
		db.lbatch = new(leveldb.Batch)
	}
	return db.lbatch
}

func (db *LevelDb) processBatches() error {
	var err error

	if len(db.txUpdateMap) != 0 || len(db.txSpentUpdateMap) != 0 || db.lbatch != nil {
		if db.lbatch == nil {
			db.lbatch = new(leveldb.Batch)
		}

		defer db.lbatch.Reset()

		for txSha, txU := range db.txUpdateMap {
			key := shaTxToKey(&txSha)
			if txU.delete {
				//log.Tracef("deleting tx %v", txSha)
				db.lbatch.Delete(key)
			} else {
				//log.Tracef("inserting tx %v", txSha)
				txdat := db.formatTx(txU)
				db.lbatch.Put(key, txdat)
			}
		}
		for txSha, txSu := range db.txSpentUpdateMap {
			key := shaSpentTxToKey(&txSha)
			if txSu.delete {
				//log.Tracef("deleting tx %v", txSha)
				db.lbatch.Delete(key)
			} else {
				//log.Tracef("inserting tx %v", txSha)
				txdat := db.formatTxFullySpent(txSu.txl)
				db.lbatch.Put(key, txdat)
			}
		}

		err = db.lDb.Write(db.lbatch, db.wo)
		if err != nil {
			log.Tracef("batch failed %v\n", err)
			return err
		}
		db.txUpdateMap = map[btcwire.ShaHash]*txUpdateObj{}
		db.txSpentUpdateMap = make(map[btcwire.ShaHash]*spentTxUpdate)
	}

	return nil
}

// RollbackClose this is part of the btcdb.Db interface and should discard
// recent changes to the db and the close the db.  This currently just does
// a clean shutdown.
func (db *LevelDb) RollbackClose() error {
	db.dbLock.Lock()
	defer db.dbLock.Unlock()

	return db.close()
}

func (db *LevelDb) NameDB() btcnamedb.DB {
  return db.nameDB
}
