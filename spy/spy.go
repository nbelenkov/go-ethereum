package spy

import (
	"encoding/hex"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/p2p"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Spy struct {
	peerCh      chan *EthereumPeer
	blockCh     chan *EthereumBlock
	txCh        chan *EthereumTransaction
	txContentCh chan *EthereumTransactionContent
}

func NewSpy() *Spy {
	spy := Spy{
		peerCh:      make(chan *EthereumPeer, 10000),
		blockCh:     make(chan *EthereumBlock, 10000),
		txCh:        make(chan *EthereumTransaction, 10000),
		txContentCh: make(chan *EthereumTransactionContent, 10000),
	}
	go spy.execute()
	return &spy
}

// closing all channels
func (w *Spy) Close() {
	close(w.peerCh)
	close(w.blockCh)
	close(w.txCh)
	close(w.txContentCh)
}

type EthereumPeer struct {
	ID           uint `gorm:"primarykey"`
	PeerID       string
	Version      int
	IP           string
	ReceivedTime time.Time
}

type EthereumBlock struct {
	ID           uint `gorm:"primarykey"`
	PeerID       string
	Hash         string `gorm:"index"`
	Code         uint
	ReceivedTime time.Time
	BlockNumber  uint
}

type EthereumTransaction struct {
	ID           uint `gorm:"primarykey"`
	PeerID       string
	Hash         string `gorm:"index"`
	Code         uint
	ReceivedTime time.Time
}

type EthereumTransactionContent struct {
	Hash     string `gorm:"primaryKey"`
	To       string
	From     string
	Nonce    uint
	Value    string
	GasPrice string
	Gas      uint
	Data     string
}

// execute is called on initialization
func (w *Spy) execute() {
	// initiate all databases
	dsn := "host=localhost user=postgres password=password dbname=postgres port=5433 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	if db.AutoMigrate(&EthereumBlock{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&EthereumTransaction{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&EthereumPeer{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&EthereumTransactionContent{}) != nil {
		panic("Failed to migrate db")
	}

	// transaction cache
	var transactionCountCacheHashes []string
	transactionCountCache := map[string]int64{}

	max := int64(20)
	maxTransactionCacheCount := 10000
	batchSize := 1000

	var blockBatch []*EthereumBlock
	var transactionBatch []*EthereumTransaction

	for {
		select {
		case block := <-w.blockCh:
			blockBatch = append(blockBatch, block)
			// batch insert if it is over the batch size
			if len(blockBatch) > batchSize {
				db.Create(&blockBatch)
				blockBatch = []*EthereumBlock{}
			}
		case tx := <-w.txCh:
			_, exists := transactionCountCache[tx.Hash]
			if !exists {
				var result []EthereumTransaction
				var count int64
				db.Where("hash = ?", tx.Hash).Find(&result).Count(&count)
				transactionCountCacheHashes = append(transactionCountCacheHashes, tx.Hash)
				transactionCountCache[tx.Hash] = count
				if len(transactionCountCacheHashes) > maxTransactionCacheCount {
					toDelete := transactionCountCacheHashes[0]
					transactionCountCacheHashes = transactionCountCacheHashes[1:]
					delete(transactionCountCache, toDelete)
				}
			}
			if transactionCountCache[tx.Hash] < max {
				transactionBatch = append(transactionBatch, tx)
				transactionCountCache[tx.Hash] += 1
				// batch insert if it is over the batch size
				if len(transactionBatch) > batchSize {
					db.Create(&transactionBatch)
					transactionBatch = []*EthereumTransaction{}
				}
			}
		case peer := <-w.peerCh:
			db.Create(&peer)
		case content := <-w.txContentCh:
			db.Clauses(clause.OnConflict{DoNothing: true}).Create(&content)
		default:
			continue
		}
	}
}

func (w *Spy) handleBlockMsg(peer *eth.Peer, msg p2p.Msg, hash string, blockNumber uint64) {
	w.blockCh <- &EthereumBlock{
		PeerID:       peer.ID(),
		Hash:         hash,
		Code:         uint(msg.Code),
		ReceivedTime: msg.ReceivedAt,
		BlockNumber:  uint(blockNumber),
	}
}

func (w *Spy) handleTxMsg(peer *eth.Peer, msg p2p.Msg, hash string) {
	w.txCh <- &EthereumTransaction{
		PeerID:       peer.ID(),
		Hash:         hash,
		Code:         uint(msg.Code),
		ReceivedTime: msg.ReceivedAt,
	}
}

func (w *Spy) handlePeerMsg(peer *eth.Peer, version int, ip string) {
	w.peerCh <- &EthereumPeer{
		PeerID:       peer.ID(),
		Version:      version,
		IP:           ip,
		ReceivedTime: time.Now(),
	}
}

func (w *Spy) HandleTxContent(hash string, msg *types.Message) {
	var toAddress string
	if msg.To() == nil {
		toAddress = ""
	} else {
		toAddress = msg.To().Hex()
	}

	w.txContentCh <- &EthereumTransactionContent{
		Hash:     hash,
		To:       toAddress,
		From:     msg.From().Hex(),
		Nonce:    uint(msg.Nonce()),
		Value:    msg.Value().String(),
		GasPrice: msg.GasPrice().String(),
		Gas:      uint(msg.Gas()),
		Data:     hex.EncodeToString(msg.Data()),
	}
}

/**
ETH wire protocol(https://github.com/ethereum/devp2p/blob/master/caps/eth.md):
0x01 NewBlockHashesMsg                     NewBlockHashesPacket              backend  [[blockhash₁: B_32, number₁: P], [blockhash₂: B_32, number₂: P], ...]
0x02 TransactionsMsg                       TransactionsPacket                backend  [tx₁, tx₂, ...]
0x03 GetBlockHeadersMsg                    GetBlockHeadersPacket             peer     [request-id: P, [startblock: {P, B_32}, limit: P, skip: P, reverse: {0, 1}]]
0x04 BlockHeadersMsg                       BlockHeadersPacket                backend  [request-id: P, [header₁, header₂, ...]]
0x05 GetBlockBodiesMsg                     GetBlockBodiesPacket              peer     [request-id: P, [blockhash₁: B_32, blockhash₂: B_32, ...]]
0x06 BlockBodiesMsg                        BlockBodiesPacket                 backend  [request-id: P, [block-body₁, block-body₂, ...]]
0x07 NewBlockMsg                           NewBlockPacket                    backend  [block, td: P]
0x08 NewPooledTransactionHashesMsg - ETH65 NewPooledTransactionHashesPacket  backend  [txhash₁: B_32, txhash₂: B_32, ...]
0x09 GetPooledTransactionsMsg - ETH65      GetPooledTransactionsPacket       peer     [request-id: P, [txhash₁: B_32, txhash₂: B_32, ...]]
0x0a PooledTransactionsMsg - ETH65         PooledTransactionsPacket          backend  [request-id: P, [tx₁, tx₂...]]
0x0d GetNodeDataMsg                        GetNodeDataPacket                 peer     [request-id: P, [hash₁: B_32, hash₂: B_32, ...]]
0x0e NodeDataMsg                           NodeDataPacket                    backend  [request-id: P, [value₁: B, value₂: B, ...]]
0x0f GetReceiptsMsg                        GetReceiptsPacket                 peer     [request-id: P, [blockhash₁: B_32, blockhash₂: B_32, ...]]
0x10 ReceiptsMsg                           ReceiptsPacket                    backend  [request-id: P, [[receipt₁, receipt₂], ...]]
*/

// New block messages
func (w *Spy) Handle0x01NewBlockHashesMsg(peer *eth.Peer, msg p2p.Msg, packet *eth.NewBlockHashesPacket) {
	for _, block := range *packet {
		w.handleBlockMsg(peer, msg, block.Hash.Hex(), block.Number)
	}
}

func (w *Spy) Handle0x07NewBlockMsg(peer *eth.Peer, msg p2p.Msg, packet *eth.NewBlockPacket) {
	w.handleBlockMsg(peer, msg, (*packet).Block.Hash().Hex(), (*packet).Block.Number().Uint64())
}

// New transaction messages
func (w *Spy) Handle0x02TransactionsMsg(peer *eth.Peer, msg p2p.Msg, packet *eth.TransactionsPacket) {
	for _, tx := range *packet {
		w.handleTxMsg(peer, msg, tx.Hash().Hex())
	}
}

func (w *Spy) Handle0x08NewPooledTransactionHashesMsg(peer *eth.Peer, msg p2p.Msg, packet *eth.NewPooledTransactionHashesPacket) {
	for _, txHash := range *packet {
		w.handleTxMsg(peer, msg, txHash.Hex())
	}
}

func (w *Spy) Handle0x09GetPooledTranscationsMsg(peer *eth.Peer, msg p2p.Msg, packet *eth.PooledTransactionsPacket) {
	for _, tx := range *packet {
		w.handleTxMsg(peer, msg, tx.Hash().Hex())
	}
}
