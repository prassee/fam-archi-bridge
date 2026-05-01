import psycopg2
from faker import Faker
import random
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import threading

fake = Faker('en_IN')

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres',
    'database': 'postgres'
}

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
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
        """)
        conn.commit()

def generate_transaction():
    now = datetime.now()
    amount = round(random.uniform(10, 100000), 2)
    processing_fee = round(amount * 0.002, 2)
    gst_amount = round(processing_fee * 0.18, 2)
    total_amount = round(amount + processing_fee + gst_amount, 2)

    return (
        fake.uuid4(),
        f"{fake.user_name()}@ybl",
        f"{fake.user_name()}@okicici",
        fake.name(),
        fake.name(),
        amount,
        now - timedelta(seconds=random.randint(0, 86400)),
        random.choice(['SUCCESS', 'FAILED', 'PENDING']),
        random.choice(['COLLECT', 'PAY']),
        random.choice(['RETAIL', 'GROCERY', 'FUEL', 'TRAVEL', 'UTILITIES']),
        fake.company(),
        fake.bban(),
        fake.bban(),
        fake.swift(),
        fake.swift(),
        fake.uuid4(),
        random.choice(['00', '01', '99']),
        fake.sentence(3),
        fake.company(),
        random.choice(['GooglePay', 'PhonePe', 'Paytm', 'BHIM']),
        fake.uuid4(),
        fake.uuid4(),
        random.choice(['MOBILE', 'WEB', 'UPI_APP']),
        fake.latitude(),
        fake.longitude(),
        fake.ipv4(),
        fake.user_agent(),
        fake.word() + '.' + str(random.randint(1,5)),
        random.choice(['Android', 'iOS']),
        fake.word() + str(random.randint(10,15)),
        fake.word(),
        fake.company(),
        random.choice(['4G', '5G', 'WIFI']),
        fake.company(),
        random.choice(['QR', 'INTENT', 'COLLECT']),
        fake.sha256(),
        random.randint(0, 3),
        processing_fee,
        gst_amount,
        total_amount,
        random.choice(['SETTLED', 'PENDING', 'FAILED']),
        (now + timedelta(days=random.randint(1,3))).date() if random.random() > 0.3 else None,
        random.choice(['NONE', 'INITIATED', 'COMPLETED']),
        round(random.uniform(10, 1000), 2) if random.random() > 0.7 else None,
        fake.uuid4(),
        fake.uuid4()
    )

def insert_batch(batch, thread_id):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO upi_transactions (
                    transaction_id, sender_upi_id, receiver_upi_id, sender_name, receiver_name,
                    amount, transaction_timestamp, status, transaction_type, merchant_category,
                    merchant_name, payer_account_number, payee_account_number, payer_ifsc, payee_ifsc,
                    transaction_ref_id, response_code, response_message, bank_name, psp_name,
                    upi_transaction_ref, device_id, channel, location_latitude, location_longitude,
                    ip_address, user_agent, app_version, os_type, os_version,
                    device_model, device_manufacturer, network_type, carrier, transaction_mode,
                    checksum, retry_count, processing_fee, gst_amount, total_amount,
                    settlement_status, settlement_date, refund_status, refund_amount, merchant_id, terminal_id
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (transaction_id) DO NOTHING
            """, batch)
            conn.commit()
        conn.close()
        return len(batch)
    except Exception as e:
        print(f"Thread {thread_id} error: {e}")
        return 0

def generate_load(target_per_sec=10000):
    batch_size = 500
    num_threads = 10
    interval = batch_size / target_per_sec

    conn = psycopg2.connect(**DB_CONFIG)
    create_table(conn)
    conn.close()
    print(f"Table created. Generating {target_per_sec} txns/sec continuously...")

    total_inserted = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        try:
            while True:
                batch_start = time.time()
                batch = [generate_transaction() for _ in range(batch_size)]
                futures.append(executor.submit(insert_batch, batch, len(futures)))

                elapsed = time.time() - batch_start
                sleep_time = interval - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

                # Collect completed futures periodically
                done_futures = [f for f in futures if f.done()]
                for future in done_futures:
                    total_inserted += future.result()
                    futures.remove(future)

        except KeyboardInterrupt:
            print("\nStopping... Waiting for pending inserts to complete.")
            for future in futures:
                total_inserted += future.result()

    elapsed = time.time() - start_time
    print(f"Inserted {total_inserted} transactions in {elapsed:.2f}s ({total_inserted/elapsed:.0f} txns/sec)")

if __name__ == "__main__":
    generate_load(target_per_sec=10000)
