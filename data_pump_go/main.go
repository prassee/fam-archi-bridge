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
	batchSize    = 500
	numWorkers   = 10
	targetPerSec = 10000

	// Table-specific constants
	usersBatchSize         = 500
	subscriptionsPerInsert = 4000
	offersPerInsert        = 100

	// Update intervals
	updateInterval       = 5 * time.Minute
	offersInsertInterval = 1 * time.Hour
)

type Transaction struct {
	TransactionID        string
	SenderUPIID          string
	ReceiverUPIID        string
	SenderName           string
	ReceiverName         string
	Amount               float64
	TransactionTimestamp time.Time
	Status               string
	TransactionType      string
	MerchantCategory     string
	MerchantName         string
	PayerAccountNumber   string
	PayeeAccountNumber   string
	PayerIFSC            string
	PayeeIFSC            string
	TransactionRefID     string
	ResponseCode         string
	ResponseMessage      string
	BankName             string
	PSPName              string
	UPITransactionRef    string
	DeviceID             string
	Channel              string
	LocationLatitude     float64
	LocationLongitude    float64
	IPAddress            string
	UserAgent            string
	AppVersion           string
	OSType               string
	OSVersion            string
	DeviceModel          string
	DeviceManufacturer   string
	NetworkType          string
	Carrier              string
	TransactionMode      string
	Checksum             string
	RetryCount           int
	ProcessingFee        float64
	GSTAmount            float64
	TotalAmount          float64
	SettlementStatus     string
	SettlementDate       *time.Time
	RefundStatus         string
	RefundAmount         *float64
	MerchantID           string
	TerminalID           string
	CreatedAt            time.Time
}

type User struct {
	UserID       string
	SenderName   string
	ReceiverName string
	Email        string
	Phone        string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type UserSubscription struct {
	SubscriptionID string
	UserID         string
	Tier           string
	BillingCycle   string
	StartDate      time.Time
	EndDate        time.Time
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Offer struct {
	OfferID     string
	Name        string
	Description string
	DiscountPct float64
	MinAmount   float64
	ValidFrom   time.Time
	ValidTo     time.Time
	IsActive    bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
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
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			user_id VARCHAR(50) PRIMARY KEY,
			sender_name VARCHAR(100),
			receiver_name VARCHAR(100),
			email VARCHAR(100),
			phone VARCHAR(20),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS user_subscription (
			subscription_id VARCHAR(50) PRIMARY KEY,
			user_id VARCHAR(50),
			tier VARCHAR(20),
			billing_cycle VARCHAR(20),
			start_date DATE,
			end_date DATE,
			status VARCHAR(20),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS offers (
			offer_id VARCHAR(50) PRIMARY KEY,
			name VARCHAR(100),
			description TEXT,
			discount_pct DECIMAL(5,2),
			min_amount DECIMAL(15,2),
			valid_from DATE,
			valid_to DATE,
			is_active BOOLEAN DEFAULT TRUE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP
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
		TransactionID:        gofakeit.UUID(),
		SenderUPIID:          fmt.Sprintf("%s@ybl", gofakeit.Username()),
		ReceiverUPIID:        fmt.Sprintf("%s@okicici", gofakeit.Username()),
		SenderName:           gofakeit.Name(),
		ReceiverName:         gofakeit.Name(),
		Amount:               amount,
		TransactionTimestamp: now.Add(-time.Duration(rand.Intn(86400)) * time.Second),
		Status:               status,
		TransactionType:      txnType,
		MerchantCategory:     merchantCategory,
		MerchantName:         gofakeit.Company(),
		PayerAccountNumber:   gofakeit.Numerify("############"),
		PayeeAccountNumber:   gofakeit.Numerify("############"),
		PayerIFSC:            gofakeit.Numerify("????0######"),
		PayeeIFSC:            gofakeit.Numerify("????0######"),
		TransactionRefID:     gofakeit.UUID(),
		ResponseCode:         randomChoice([]string{"00", "01", "99"}),
		ResponseMessage:      gofakeit.Sentence(3),
		BankName:             gofakeit.Company(),
		PSPName:              pspName,
		UPITransactionRef:    gofakeit.UUID(),
		DeviceID:             gofakeit.UUID(),
		Channel:              channel,
		LocationLatitude:     round(rand.Float64()*180-90, 8),
		LocationLongitude:    round(rand.Float64()*360-180, 8),
		IPAddress:            gofakeit.IPv4Address(),
		UserAgent:            gofakeit.UserAgent(),
		AppVersion:           fmt.Sprintf("%s.%d", gofakeit.Word(), rand.Intn(5)+1),
		OSType:               osType,
		OSVersion:            fmt.Sprintf("%d", rand.Intn(5)+10),
		DeviceModel:          gofakeit.Word(),
		DeviceManufacturer:   gofakeit.Company(),
		NetworkType:          networkType,
		Carrier:              gofakeit.Company(),
		TransactionMode:      txnMode,
		Checksum:             fmt.Sprintf("%x", gofakeit.LetterN(32)),
		RetryCount:           rand.Intn(4),
		ProcessingFee:        processingFee,
		GSTAmount:            gstAmount,
		TotalAmount:          totalAmount,
		SettlementStatus:     settlementStatus,
		SettlementDate:       settlementDate,
		RefundStatus:         refundStatus,
		RefundAmount:         refundAmount,
		MerchantID:           gofakeit.UUID(),
		TerminalID:           gofakeit.UUID(),
		CreatedAt:            now,
	}
}

func generateUser() User {
	now := time.Now()
	return User{
		UserID:       gofakeit.UUID(),
		SenderName:   gofakeit.FirstName(),
		ReceiverName: gofakeit.LastName(),
		Email:        gofakeit.Email(),
		Phone:        gofakeit.Phone(),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

func generateSubscription() UserSubscription {
	now := time.Now()
	tier := randomChoice([]string{"MONTHLY", "YEARLY"})
	billingCycle := tier
	startDate := now
	endDate := now.AddDate(0, 0, 30)
	if tier == "YEARLY" {
		endDate = now.AddDate(1, 0, 0)
	}
	status := randomChoice([]string{"ACTIVE", "EXPIRED", "CANCELLED"})

	return UserSubscription{
		SubscriptionID: gofakeit.UUID(),
		UserID:         gofakeit.UUID(),
		Tier:           tier,
		BillingCycle:   billingCycle,
		StartDate:      startDate,
		EndDate:        endDate,
		Status:         status,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func generateOffer() Offer {
	now := time.Now()
	return Offer{
		OfferID:     gofakeit.UUID(),
		Name:        gofakeit.Word(),
		Description: gofakeit.Sentence(10),
		DiscountPct: round(rand.Float64()*50, 2),
		MinAmount:   round(rand.Float64()*1000+100, 2),
		ValidFrom:   now,
		ValidTo:     now.AddDate(0, 1, 0),
		IsActive:    rand.Float64() > 0.2,
		CreatedAt:   now,
		UpdatedAt:   now,
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

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
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

func insertUsersBatch(ctx context.Context, pool *pgxpool.Pool, batch []User) int {
	if len(batch) == 0 {
		return 0
	}

	_, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{"users"},
		[]string{"user_id", "sender_name", "receiver_name", "email", "phone", "created_at", "updated_at"},
		pgx.CopyFromSlice(len(batch), func(i int) ([]interface{}, error) {
			u := batch[i]
			return []interface{}{u.UserID, u.SenderName, u.ReceiverName, u.Email, u.Phone, u.CreatedAt, u.UpdatedAt}, nil
		}),
	)

	if err != nil {
		fmt.Printf("Users batch insert error: %v\n", err)
		return 0
	}

	return len(batch)
}

func insertSubscriptionsBatch(ctx context.Context, pool *pgxpool.Pool, batch []UserSubscription) int {
	if len(batch) == 0 {
		return 0
	}

	_, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{"user_subscription"},
		[]string{"subscription_id", "user_id", "tier", "billing_cycle", "start_date", "end_date", "status", "created_at", "updated_at"},
		pgx.CopyFromSlice(len(batch), func(i int) ([]interface{}, error) {
			s := batch[i]
			return []interface{}{s.SubscriptionID, s.UserID, s.Tier, s.BillingCycle, s.StartDate, s.EndDate, s.Status, s.CreatedAt, s.UpdatedAt}, nil
		}),
	)

	if err != nil {
		fmt.Printf("Subscriptions batch insert error: %v\n", err)
		return 0
	}

	return len(batch)
}

func insertOffersBatch(ctx context.Context, pool *pgxpool.Pool, batch []Offer) int {
	if len(batch) == 0 {
		return 0
	}

	_, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{"offers"},
		[]string{"offer_id", "name", "description", "discount_pct", "min_amount", "valid_from", "valid_to", "is_active", "created_at", "updated_at"},
		pgx.CopyFromSlice(len(batch), func(i int) ([]interface{}, error) {
			o := batch[i]
			return []interface{}{o.OfferID, o.Name, o.Description, o.DiscountPct, o.MinAmount, o.ValidFrom, o.ValidTo, o.IsActive, o.CreatedAt, o.UpdatedAt}, nil
		}),
	)

	if err != nil {
		fmt.Printf("Offers batch insert error: %v\n", err)
		return 0
	}

	return len(batch)
}

func generateLoad(ctx context.Context, pool *pgxpool.Pool) {
	interval := float64(batchSize) / float64(targetPerSec)

	var totalInserted atomic.Int64
	var totalUpdated atomic.Int64
	var totalUsersInserted atomic.Int64
	var totalSubsInserted atomic.Int64
	var totalOffersInserted atomic.Int64
	var totalUsersUpdated atomic.Int64
	var totalSubsUpdated atomic.Int64
	startTime := time.Now()

	// Channel for UPI transactions
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

	// Channel for users (fast moving)
	usersChan := make(chan []User, numWorkers)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range usersChan {
				inserted := insertUsersBatch(ctx, pool, batch)
				totalUsersInserted.Add(int64(inserted))
			}
		}()
	}

	// Channel for subscriptions (medium moving)
	subsChan := make(chan []UserSubscription, 5)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range subsChan {
				inserted := insertSubscriptionsBatch(ctx, pool, batch)
				totalSubsInserted.Add(int64(inserted))
			}
		}()
	}

	// Channel for offers (slow moving)
	offersChan := make(chan []Offer, 5)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range offersChan {
				inserted := insertOffersBatch(ctx, pool, batch)
				totalOffersInserted.Add(int64(inserted))
			}
		}()
	}

	// Update goroutines - users: 20-50% of records every 5 minutes
	userUpdateTicker := time.NewTicker(updateInterval)
	defer userUpdateTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-userUpdateTicker.C:
				// Get total count and update 20-50%
				var count int
				pool.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
				if count > 0 {
					updatePct := 0.2 + rand.Float64()*0.3 // 20-50%
					numUpdates := int(float64(count) * updatePct)
					if numUpdates < 100 {
						numUpdates = 100
					}
					updated := updateRandomUsers(ctx, pool, numUpdates)
					totalUsersUpdated.Add(int64(updated))
					fmt.Printf("Updated %d user records (total user updates: %d)\n", updated, totalUsersUpdated.Load())
				}
			}
		}
	}()

	// Update goroutines - subscriptions: 20-50% of records every 5 minutes
	subUpdateTicker := time.NewTicker(updateInterval)
	defer subUpdateTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-subUpdateTicker.C:
				var count int
				pool.QueryRow(ctx, "SELECT COUNT(*) FROM user_subscription").Scan(&count)
				if count > 0 {
					updatePct := 0.2 + rand.Float64()*0.3
					numUpdates := int(float64(count) * updatePct)
					if numUpdates < 100 {
						numUpdates = 100
					}
					updated := updateRandomSubscriptions(ctx, pool, numUpdates)
					totalSubsUpdated.Add(int64(updated))
					fmt.Printf("Updated %d subscription records (total sub updates: %d)\n", updated, totalSubsUpdated.Load())
				}
			}
		}
	}()

	// Update goroutines - UPI transactions: 20-50% of records every 5 minutes
	txnUpdateTicker := time.NewTicker(updateInterval)
	defer txnUpdateTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-txnUpdateTicker.C:
				var count int
				pool.QueryRow(ctx, "SELECT COUNT(*) FROM upi_transactions").Scan(&count)
				if count > 0 {
					updatePct := 0.2 + rand.Float64()*0.3
					numUpdates := int(float64(count) * updatePct)
					if numUpdates < 100 {
						numUpdates = 100
					}
					updated := updateRandomRecords(ctx, pool, numUpdates)
					totalUpdated.Add(int64(updated))
					fmt.Printf("Updated %d transaction records (total updates: %d)\n", updated, totalUpdated.Load())
				}
			}
		}
	}()

	// Subscription insert ticker - 4000 every 5 minutes
	subInsertTicker := time.NewTicker(updateInterval)
	defer subInsertTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-subInsertTicker.C:
				batch := make([]UserSubscription, subscriptionsPerInsert)
				for i := 0; i < subscriptionsPerInsert; i++ {
					batch[i] = generateSubscription()
				}
				subsChan <- batch
			}
		}
	}()

	// Offers insert ticker - 100 every 1 hour
	offersTicker := time.NewTicker(offersInsertInterval)
	defer offersTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-offersTicker.C:
				batch := make([]Offer, offersPerInsert)
				for i := 0; i < offersPerInsert; i++ {
					batch[i] = generateOffer()
				}
				offersChan <- batch
			}
		}
	}()

	fmt.Printf("Tables created. Generating load:\n")
	fmt.Printf("  - UPI transactions: %d txns/sec\n", targetPerSec)
	fmt.Printf("  - Users: %d txns/sec (same as UPI)\n", targetPerSec)
	fmt.Printf("  - Subscriptions: %d every 5 minutes\n", subscriptionsPerInsert)
	fmt.Printf("  - Offers: %d every 1 hour\n", offersPerInsert)
	fmt.Println("Updates: 20-50% of users and subscriptions every 5 minutes")

	// Main insert loop for UPI transactions and users (fast moving)
	ticker := time.NewTicker(time.Duration(interval*1000) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(workChan)
			close(usersChan)
			close(subsChan)
			close(offersChan)
			wg.Wait()
			elapsed := time.Since(startTime).Seconds()
			fmt.Printf("\n=== Summary ===\n")
			fmt.Printf("UPI transactions inserted: %d in %.2fs (%.0f txns/sec)\n",
				totalInserted.Load(), elapsed, float64(totalInserted.Load())/elapsed)
			fmt.Printf("Users inserted: %d\n", totalUsersInserted.Load())
			fmt.Printf("Subscriptions inserted: %d\n", totalSubsInserted.Load())
			fmt.Printf("Offers inserted: %d\n", totalOffersInserted.Load())
			fmt.Printf("Total records updated: %d (UPI), %d (users), %d (subs)\n",
				totalUpdated.Load(), totalUsersUpdated.Load(), totalSubsUpdated.Load())
			return
		case <-ticker.C:
			// UPI transactions batch
			batch := make([]Transaction, batchSize)
			for i := 0; i < batchSize; i++ {
				batch[i] = generateTransaction()
			}
			workChan <- batch

			// Users batch (same rate as UPI transactions)
			userBatch := make([]User, usersBatchSize)
			for i := 0; i < usersBatchSize; i++ {
				userBatch[i] = generateUser()
			}
			usersChan <- userBatch
		}
	}
}

func updateRandomRecords(ctx context.Context, pool *pgxpool.Pool, count int) int {
	rows, err := pool.Query(ctx, `
		SELECT transaction_id FROM upi_transactions
		TABLESAMPLE SYSTEM (10)
		LIMIT $1
	`, count)
	if err != nil {
		fmt.Printf("Failed to get random transaction IDs: %v\n", err)
		return 0
	}
	defer rows.Close()

	var txnIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			fmt.Printf("Failed to scan transaction ID: %v\n", err)
			continue
		}
		txnIDs = append(txnIDs, id)
	}

	if len(txnIDs) == 0 {
		return 0
	}

	now := time.Now()
	status := randomChoice([]string{"SUCCESS", "FAILED", "PENDING"})
	settlementStatus := randomChoice([]string{"SETTLED", "PENDING", "FAILED"})
	refundStatus := randomChoice([]string{"NONE", "INITIATED", "COMPLETED"})
	amount := round(rand.Float64()*100000+10, 2)
	timestamp := now.Add(-time.Duration(rand.Intn(86400)) * time.Second)

	cmdTag, err := pool.Exec(ctx, `
		UPDATE upi_transactions
		SET status = $1,
			settlement_status = $2,
			refund_status = $3,
			amount = $4,
			transaction_timestamp = $5,
			updated_at = $6
		WHERE transaction_id = ANY($7)
	`, status, settlementStatus, refundStatus, amount, timestamp, now, txnIDs)
	if err != nil {
		fmt.Printf("Failed to update transactions: %v\n", err)
		return 0
	}

	return int(cmdTag.RowsAffected())
}

func updateRandomUsers(ctx context.Context, pool *pgxpool.Pool, count int) int {
	rows, err := pool.Query(ctx, `
		SELECT user_id FROM users
		TABLESAMPLE SYSTEM (10)
		LIMIT $1
	`, count)
	if err != nil {
		fmt.Printf("Failed to get random user IDs: %v\n", err)
		return 0
	}
	defer rows.Close()

	var userIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		userIDs = append(userIDs, id)
	}

	if len(userIDs) == 0 {
		return 0
	}

	now := time.Now()
	updated := 0

	for _, id := range userIDs {
		_, err := pool.Exec(ctx, `
			UPDATE users
			SET sender_name = $1,
				receiver_name = $2,
				email = $3,
				phone = $4,
				updated_at = $5
			WHERE user_id = $6
		`, gofakeit.FirstName(), gofakeit.LastName(), gofakeit.Email(), gofakeit.Phone(), now, id)

		if err != nil {
			continue
		}
		updated++
	}

	return updated
}

func updateRandomSubscriptions(ctx context.Context, pool *pgxpool.Pool, count int) int {
	rows, err := pool.Query(ctx, `
		SELECT subscription_id FROM user_subscription
		TABLESAMPLE SYSTEM (10)
		LIMIT $1
	`, count)
	if err != nil {
		fmt.Printf("Failed to get random subscription IDs: %v\n", err)
		return 0
	}
	defer rows.Close()

	var subIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		subIDs = append(subIDs, id)
	}

	if len(subIDs) == 0 {
		return 0
	}

	now := time.Now()
	updated := 0

	for _, id := range subIDs {
		status := randomChoice([]string{"ACTIVE", "EXPIRED", "CANCELLED"})
		_, err := pool.Exec(ctx, `
			UPDATE user_subscription
			SET status = $1,
				tier = $2,
				end_date = $3,
				updated_at = $4
			WHERE subscription_id = $5
		`, status, randomChoice([]string{"MONTHLY", "YEARLY"}),
			now.AddDate(0, rand.Intn(12)+1, 0), now, id)

		if err != nil {
			continue
		}
		updated++
	}

	return updated
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	// Read database config from environment variables
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "postgres")
	dbPassword := getEnv("DB_PASSWORD", "postgres")
	dbName := getEnv("DB_NAME", "postgres")

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		fmt.Printf("Failed to parse database config: %v\n", err)
		return
	}
	cfg.MaxConns = 30

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
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
