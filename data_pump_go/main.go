package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	batchSize     = 500
	numWorkers    = 10
	targetPerSec  = 10000
)

type Transaction struct {
	TransactionID       string
	SenderUPIID         string
	ReceiverUPIID       string
	SenderName          string
	ReceiverName        string
	Amount              float64
	TransactionTimestamp time.Time
	Status              string
	TransactionType     string
	MerchantCategory    string
	MerchantName        string
	PayerAccountNumber  string
	PayeeAccountNumber  string
	PayerIFSC           string
	PayeeIFSC           string
	TransactionRefID    string
	ResponseCode        string
	ResponseMessage     string
	BankName            string
	PSPName             string
	UPITransactionRef   string
	DeviceID            string
	Channel             string
	LocationLatitude    float64
	LocationLongitude   float64
	IPAddress           string
	UserAgent           string
	AppVersion          string
	OSType              string
	OSVersion           string
	DeviceModel         string
	DeviceManufacturer  string
	NetworkType         string
	Carrier             string
	TransactionMode     string
	Checksum            string
	RetryCount          int
	ProcessingFee       float64
	GSTAmount           float64
	TotalAmount         float64
	SettlementStatus    string
	SettlementDate      *time.Time
	RefundStatus        string
	RefundAmount        *float64
	MerchantID          string
	TerminalID          string
	CreatedAt           time.Time
}

func createTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS upi_transactions (
			transaction_id VARCHAR(50) PRIMARY KEY,
			sender_upi_id VARCHAR(100),
			receiver_upi_id VARCHAR(100),
			sender_name VARCHAR(100),
			receiver_name VARCHAR(100),
			amount DECIMAL(15,2),
			transaction_timestamp TIMESTAMP,
			status VARCHAR(20),
			transaction_type VARCHAR(20),
			merchant_category VARCHAR(50),
			merchant_name VARCHAR(100),
			payer_account_number VARCHAR(50),
			payee_account_number VARCHAR(50),
			payer_ifsc VARCHAR(20),
			payee_ifsc VARCHAR(20),
			transaction_ref_id VARCHAR(50),
			response_code VARCHAR(10),
			response_message VARCHAR(200),
			bank_name VARCHAR(100),
			psp_name VARCHAR(50),
			upi_transaction_ref VARCHAR(50),
			device_id VARCHAR(100),
			channel VARCHAR(20),
			location_latitude DECIMAL(10,8),
			location_longitude DECIMAL(11,8),
			ip_address INET,
			user_agent TEXT,
			app_version VARCHAR(20),
			os_type VARCHAR(20),
			os_version VARCHAR(20),
			device_model VARCHAR(50),
			device_manufacturer VARCHAR(50),
			network_type VARCHAR(20),
			carrier VARCHAR(50),
			transaction_mode VARCHAR(20),
			checksum VARCHAR(100),
			retry_count INTEGER,
			processing_fee DECIMAL(10,2),
			gst_amount DECIMAL(10,2),
			total_amount DECIMAL(15,2),
			settlement_status VARCHAR(20),
			settlement_date DATE,
			refund_status VARCHAR(20),
			refund_amount DECIMAL(15,2),
			merchant_id VARCHAR(50),
			terminal_id VARCHAR(50),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	return err
}

func generateTransaction() Transaction {
	now := time.Now()
	amount := round(rand.Float64()*100000+10, 2)
	processingFee := round(amount*0.002, 2)
	gstAmount := round(processingFee*0.18, 2)
	totalAmount := round(amount+processingFee+gstAmount, 2)

	status := randomChoice([]string{"SUCCESS", "FAILED", "PENDING"})
	txnType := randomChoice([]string{"COLLECT", "PAY"})
	merchantCategory := randomChoice([]string{"RETAIL", "GROCERY", "FUEL", "TRAVEL", "UTILITIES"})
	pspName := randomChoice([]string{"GooglePay", "PhonePe", "Paytm", "BHIM"})
	channel := randomChoice([]string{"MOBILE", "WEB", "UPI_APP"})
	osType := randomChoice([]string{"Android", "iOS"})
	networkType := randomChoice([]string{"4G", "5G", "WIFI"})
	txnMode := randomChoice([]string{"QR", "INTENT", "COLLECT"})
	settlementStatus := randomChoice([]string{"SETTLED", "PENDING", "FAILED"})
	refundStatus := randomChoice([]string{"NONE", "INITIATED", "COMPLETED"})

	var settlementDate *time.Time
	if rand.Float64() > 0.3 {
		d := now.AddDate(0, 0, rand.Intn(3)+1).Truncate(24 * time.Hour)
		settlementDate = &d
	}

	var refundAmount *float64
	if rand.Float64() > 0.7 {
		amt := round(rand.Float64()*1000+10, 2)
		refundAmount = &amt
	}

	return Transaction{
		TransactionID:       gofakeit.UUID(),
		SenderUPIID:         fmt.Sprintf("%s@ybl", gofakeit.Username()),
		ReceiverUPIID:       fmt.Sprintf("%s@okicici", gofakeit.Username()),
		SenderName:          gofakeit.Name(),
		ReceiverName:        gofakeit.Name(),
		Amount:              amount,
		TransactionTimestamp: now.Add(-time.Duration(rand.Intn(86400)) * time.Second),
		Status:              status,
		TransactionType:     txnType,
		MerchantCategory:    merchantCategory,
		MerchantName:        gofakeit.Company(),
		PayerAccountNumber:  gofakeit.Numerify("############"),
		PayeeAccountNumber:  gofakeit.Numerify("############"),
		PayerIFSC:           gofakeit.Numerify("????0######"),
		PayeeIFSC:           gofakeit.Numerify("????0######"),
		TransactionRefID:    gofakeit.UUID(),
		ResponseCode:        randomChoice([]string{"00", "01", "99"}),
		ResponseMessage:     gofakeit.Sentence(3),
		BankName:            gofakeit.Company(),
		PSPName:             pspName,
		UPITransactionRef:   gofakeit.UUID(),
		DeviceID:            gofakeit.UUID(),
		Channel:             channel,
		LocationLatitude:    round(rand.Float64()*180-90, 8),
		LocationLongitude:   round(rand.Float64()*360-180, 8),
		IPAddress:           gofakeit.IPv4Address(),
		UserAgent:           gofakeit.UserAgent(),
		AppVersion:          fmt.Sprintf("%s.%d", gofakeit.Word(), rand.Intn(5)+1),
		OSType:              osType,
		OSVersion:           fmt.Sprintf("%d", rand.Intn(5)+10),
		DeviceModel:         gofakeit.Word(),
		DeviceManufacturer:  gofakeit.Company(),
		NetworkType:         networkType,
		Carrier:             gofakeit.Company(),
		TransactionMode:     txnMode,
		Checksum:            fmt.Sprintf("%x", gofakeit.LetterN(32)),
		RetryCount:          rand.Intn(4),
		ProcessingFee:       processingFee,
		GSTAmount:           gstAmount,
		TotalAmount:         totalAmount,
		SettlementStatus:    settlementStatus,
		SettlementDate:      settlementDate,
		RefundStatus:        refundStatus,
		RefundAmount:        refundAmount,
		MerchantID:          gofakeit.UUID(),
		TerminalID:          gofakeit.UUID(),
		CreatedAt:           now,
	}
}

func round(val float64, precision int) float64 {
	pow := 1.0
	for i := 0; i < precision; i++ {
		pow *= 10
	}
	return float64(int(val*pow+0.5)) / pow
}

func randomChoice(options []string) string {
	return options[rand.Intn(len(options))]
}

func insertBatch(ctx context.Context, pool *pgxpool.Pool, batch []Transaction) int {
	if len(batch) == 0 {
		return 0
	}

	rows := make([]interface{}, len(batch))
	for i, txn := range batch {
		rows[i] = txn
	}

	_, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{"upi_transactions"},
		[]string{
			"transaction_id", "sender_upi_id", "receiver_upi_id", "sender_name", "receiver_name",
			"amount", "transaction_timestamp", "status", "transaction_type", "merchant_category",
			"merchant_name", "payer_account_number", "payee_account_number", "payer_ifsc", "payee_ifsc",
			"transaction_ref_id", "response_code", "response_message", "bank_name", "psp_name",
			"upi_transaction_ref", "device_id", "channel", "location_latitude", "location_longitude",
			"ip_address", "user_agent", "app_version", "os_type", "os_version",
			"device_model", "device_manufacturer", "network_type", "carrier", "transaction_mode",
			"checksum", "retry_count", "processing_fee", "gst_amount", "total_amount",
			"settlement_status", "settlement_date", "refund_status", "refund_amount", "merchant_id", "terminal_id",
		},
		pgx.CopyFromSlice(len(batch), func(i int) ([]interface{}, error) {
			txn := batch[i]
			return []interface{}{
				txn.TransactionID, txn.SenderUPIID, txn.ReceiverUPIID, txn.SenderName, txn.ReceiverName,
				txn.Amount, txn.TransactionTimestamp, txn.Status, txn.TransactionType, txn.MerchantCategory,
				txn.MerchantName, txn.PayerAccountNumber, txn.PayeeAccountNumber, txn.PayerIFSC, txn.PayeeIFSC,
				txn.TransactionRefID, txn.ResponseCode, txn.ResponseMessage, txn.BankName, txn.PSPName,
				txn.UPITransactionRef, txn.DeviceID, txn.Channel, txn.LocationLatitude, txn.LocationLongitude,
				txn.IPAddress, txn.UserAgent, txn.AppVersion, txn.OSType, txn.OSVersion,
				txn.DeviceModel, txn.DeviceManufacturer, txn.NetworkType, txn.Carrier, txn.TransactionMode,
				txn.Checksum, txn.RetryCount, txn.ProcessingFee, txn.GSTAmount, txn.TotalAmount,
				txn.SettlementStatus, txn.SettlementDate, txn.RefundStatus, txn.RefundAmount, txn.MerchantID, txn.TerminalID,
			}, nil
		}),
	)

	if err != nil {
		fmt.Printf("Batch insert error: %v\n", err)
		return 0
	}

	return len(batch)
}

func generateLoad(ctx context.Context, pool *pgxpool.Pool) {
	interval := float64(batchSize) / float64(targetPerSec)

	var totalInserted atomic.Int64
	startTime := time.Now()

	workChan := make(chan []Transaction, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range workChan {
				inserted := insertBatch(ctx, pool, batch)
				totalInserted.Add(int64(inserted))
			}
		}()
	}

	fmt.Printf("Table created. Generating %d txns/sec continuously...\n", targetPerSec)

	ticker := time.NewTicker(time.Duration(interval*1000) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(workChan)
			wg.Wait()
			elapsed := time.Since(startTime).Seconds()
			fmt.Printf("\nInserted %d transactions in %.2fs (%.0f txns/sec)\n",
				totalInserted.Load(), elapsed, float64(totalInserted.Load())/elapsed)
			return
		case <-ticker.C:
			batch := make([]Transaction, batchSize)
			for i := 0; i < batchSize; i++ {
				batch[i] = generateTransaction()
			}
			workChan <- batch
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		fmt.Printf("Failed to connect to database: %v\n", err)
		return
	}
	defer pool.Close()

	if err := createTable(ctx, pool); err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
		return
	}

	go func() {
		<-sigChan
		fmt.Println("\nStopping... Waiting for pending inserts to complete.")
		cancel()
	}()

	generateLoad(ctx, pool)
}
