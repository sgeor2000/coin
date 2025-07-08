import asyncio
import websockets
import json
import os
import re
import aiohttp
from datetime import datetime
from asyncio import Queue
import sqlite3
import csv

# ==============================================================================
# --- 1. ΚΕΝΤΡΙΚΕΣ ΡΥΘΜΙΣΕΙΣ ---
# ==============================================================================
CONFIG = {
    "PUMPORTAL_URI": "wss://pumpportal.fun/api/data",
    "DEXSCREENER_API_BASE": "https://api.dexscreener.com/latest/dex/tokens/",
    
    "DATABASE_FILE": "crypto_analysis.db",
    "DETAILS_LOG_FILENAME": "migration_details_log.csv",

    "UPDATE_INTERVAL_SECONDS": 300,
    "INITIAL_FETCH_DELAY_SECONDS": 10,
    "MARKET_CAP_DELETE_THRESHOLD": 15000, # Όριο Market Cap για διαγραφή
}

STATIC_KEYS = ['baseToken_address', 'baseToken_name', 'baseToken_symbol', 'pairCreatedAt', 'info_websites', 'info_socials']

# Η λίστα είναι τώρα πλήρης και σωστή.
VARIABLE_KEYS = [
    'priceUsd', 'txns_m5_buys', 'txns_m5_sells', 'txns_h1_buys', 'txns_h1_sells', 
    'txns_h6_buys', 'txns_h6_sells', 'txns_h24_buys', 'txns_h24_sells', 
    'volume_h24', 'volume_h6', 'volume_h1', 'volume_m5', 
    'priceChange_m5', 'priceChange_h1', 'priceChange_h6', 'priceChange_h24', 
    'liquidity_usd', 'marketCap', 'boosts_active'
]

# ==============================================================================
# --- 2. ΔΙΑΧΕΙΡΙΣΗ ΒΑΣΗΣ ΔΕΔΟΜΕΝΩΝ (SQLite) ---
# ==============================================================================

def init_db():
    with sqlite3.connect(CONFIG["DATABASE_FILE"]) as con:
        cur = con.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                id INTEGER PRIMARY KEY,
                mint_address TEXT NOT NULL UNIQUE,
                symbol TEXT,
                name TEXT,
                pair_created_at TEXT,
                websites TEXT,
                socials TEXT,
                first_seen_at TEXT NOT NULL
            )
        ''')
        
        # --- ΑΛΛΑΓΗ 1: Ο πίνακας metrics είναι τώρα πλήρως ενημερωμένος ---
        cur.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY,
                token_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                price_usd REAL,
                market_cap REAL,
                liquidity_usd REAL,
                volume_m5 REAL,
                volume_h1 REAL,
                volume_h6 REAL,
                volume_h24 REAL,
                price_change_m5 REAL,
                price_change_h1 REAL,
                price_change_h6 REAL,
                price_change_h24 REAL,
                txns_m5_buys INTEGER,
                txns_m5_sells INTEGER,
                txns_h1_buys INTEGER,
                txns_h1_sells INTEGER,
                boosts_active INTEGER,
                FOREIGN KEY (token_id) REFERENCES tokens (id) ON DELETE CASCADE
            )
        ''')
        con.commit()
    print("[DB] ✅ Η βάση δεδομένων είναι έτοιμη.")

def get_or_create_token(mint_address: str, static_data: dict) -> int:
    with sqlite3.connect(CONFIG["DATABASE_FILE"]) as con:
        cur = con.cursor()
        cur.execute("SELECT id FROM tokens WHERE mint_address = ?", (mint_address,))
        result = cur.fetchone()
        
        if result:
            return result[0]
        else:
            print(f"[DB] ✨ Προσθήκη νέου token στη βάση: {static_data.get('baseToken_symbol', mint_address)}")
            cur.execute('''
                INSERT INTO tokens (mint_address, symbol, name, pair_created_at, websites, socials, first_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                mint_address,
                static_data.get('baseToken_symbol'),
                static_data.get('baseToken_name'),
                static_data.get('pairCreatedAt'),
                json.dumps(static_data.get('info_websites')),
                json.dumps(static_data.get('info_socials')),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            return cur.lastrowid

def add_metrics(token_id: int, variable_data: dict):
    with sqlite3.connect(CONFIG["DATABASE_FILE"]) as con:
        cur = con.cursor()
        
        # --- ΑΛΛΑΓΗ 2: Η εντολή INSERT είναι τώρα πλήρως ενημερωμένη ---
        cur.execute('''
            INSERT INTO metrics (
                token_id, timestamp, price_usd, market_cap, liquidity_usd,
                volume_m5, volume_h1, volume_h6, volume_h24,
                price_change_m5, price_change_h1, price_change_h6, price_change_h24,
                txns_m5_buys, txns_m5_sells, txns_h1_buys, txns_h1_sells, 
                boosts_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            token_id,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            variable_data.get('priceUsd'),
            variable_data.get('marketCap'),
            variable_data.get('liquidity_usd'),
            variable_data.get('volume_m5'),
            variable_data.get('volume_h1'),
            variable_data.get('volume_h6'),
            variable_data.get('volume_h24'),
            variable_data.get('priceChange_m5'),
            variable_data.get('priceChange_h1'),
            variable_data.get('priceChange_h6'),
            variable_data.get('priceChange_h24'),
            variable_data.get('txns_m5_buys'),
            variable_data.get('txns_m5_sells'),
            variable_data.get('txns_h1_buys'),
            variable_data.get('txns_h1_sells'),
            variable_data.get('boosts_active')
        ))
        con.commit()

def delete_token_by_mint(mint_address: str):
    with sqlite3.connect(CONFIG["DATABASE_FILE"]) as con:
        cur = con.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("DELETE FROM tokens WHERE mint_address = ?", (mint_address,))
        con.commit()
        print(f"[DB] 🗑️ Το token {mint_address} και τα δεδομένα του διαγράφηκαν.")

# ==============================================================================
# --- 3. TASKS (Listener & Fetcher) ---
# Ο κώδικας εδώ δεν χρειαζόταν αλλαγές
# ==============================================================================

async def websocket_listener(mint_queue: Queue):
    uri = CONFIG["PUMPORTAL_URI"]
    log_filename = CONFIG["DETAILS_LOG_FILENAME"]
    
    file_exists = os.path.exists(log_filename)
    with open(log_filename, mode='a', newline='', encoding='utf-8') as log_file:
        log_writer = csv.writer(log_file)
        if not file_exists:
            log_writer.writerow(['Timestamp', 'Signature', 'Mint', 'Pool'])

        while True:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as websocket:
                    print("[LISTENER] 🚀 Συνδέθηκε στο PumpPortal!")
                    payload = {"method": "subscribeMigration"}
                    await websocket.send(json.dumps(payload))
                    print("[LISTENER] ✅ Έγινε εγγραφή στα MIGRATION events.")
                    
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            if 'mint' not in data or 'signature' not in data:
                                continue

                            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            mint = data.get('mint')
                            
                            await mint_queue.put(mint)
                            
                            print(f"\n🎓🔥 [{datetime.now().strftime('%H:%M:%S')}] GRADUATION DETECTED!")
                            print(f"   🪙 Mint: {mint}")
                            print(f"   QUEUEING FOR ANALYSIS...")

                            log_writer.writerow([timestamp, data.get('signature'), mint, data.get('pool')])
                            log_file.flush()

                        except json.JSONDecodeError:
                            print(f"[LISTENER] ❌ Λάθος JSON: {message}")
                        except Exception as e:
                            print(f"[LISTENER] ❌ Σφάλμα επεξεργασίας μηνύματος: {e}")

            except websockets.exceptions.ConnectionClosed as e:
                print(f"[LISTENER] ❌ Η σύνδεση έκλεισε ({e}). Επανασύνδεση σε 5 δευτ...")
            except Exception as e:
                print(f"[LISTENER] ❌ Σφάλμα σύνδεσης: {e}. Επανασύνδεση σε 5 δευτ...")
            
            await asyncio.sleep(5)

async def fetch_token_data(session: aiohttp.ClientSession, mint_address: str):
    api_url = f"{CONFIG['DEXSCREENER_API_BASE']}{mint_address}"
    try:
        async with session.get(api_url) as response:
            response.raise_for_status()
            api_data = await response.json()
            if not api_data.get('pairs'):
                return mint_address, None, "Δεν βρέθηκαν ζεύγη"
            
            flat_data = flatten_json(api_data['pairs'][0])
            return mint_address, flat_data, None
            
    except aiohttp.ClientError as e:
        return mint_address, None, f"Σφάλμα δικτύου: {e}"
    except Exception as e:
        return mint_address, None, f"Απρόσμενο σφάλμα: {e}"

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            out[name[:-1]] = json.dumps(x)
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

# ==============================================================================
# --- 4. TASK 2: DATA PROCESSOR ---
# Ο κώδικας εδώ δεν χρειαζόταν αλλαγές
# ==============================================================================

async def data_processor(mint_queue: Queue):
    tracked_mints = set()
    
    try:
        with sqlite3.connect(CONFIG["DATABASE_FILE"]) as con:
            cur = con.cursor()
            cur.execute("SELECT mint_address FROM tokens")
            results = cur.fetchall()
            for row in results:
                tracked_mints.add(row[0])
        if tracked_mints:
            print(f"[PROCESSOR] ✅ Βρέθηκαν και φορτώθηκαν {len(tracked_mints)} tokens από τη βάση δεδομένων.")
    except Exception as e:
        print(f"[PROCESSOR] ⚠️ Δεν βρέθηκαν προηγούμενα tokens στη βάση: {e}")

    waiting_message_shown = False

    await asyncio.sleep(CONFIG['INITIAL_FETCH_DELAY_SECONDS'])

    while True:
        while not mint_queue.empty():
            mint = await mint_queue.get()
            if mint not in tracked_mints:
                print(f"[PROCESSOR] ✨ Νέο token προστέθηκε στη λίστα παρακολούθησης: {mint}")
                tracked_mints.add(mint)
            mint_queue.task_done()

        if not tracked_mints:
            if not waiting_message_shown:
                print(f"[PROCESSOR] 💤 Δεν υπάρχουν tokens για παρακολούθηση. Αναμονή για το πρώτο migration...")
                waiting_message_shown = True
            await asyncio.sleep(15)
            continue
        
        waiting_message_shown = False

        print(f"\n{'='*60}")
        print(f"[PROCESSOR] 🔄 Ξεκινά κύκλος ενημέρωσης για {len(tracked_mints)} token(s) στις {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        
        tasks = []
        async with aiohttp.ClientSession() as session:
            for mint_address in list(tracked_mints):
                task = asyncio.create_task(fetch_token_data(session, mint_address))
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)

        updates_made = 0
        
        for mint_address, data, error in results:
            if error:
                print(f"   ❌ Σφάλμα για {mint_address}: {error}. Αφαίρεση από την παρακολούθηση.")
                delete_token_by_mint(mint_address)
                tracked_mints.discard(mint_address)
                continue
            
            try:
                market_cap_str = data.get('marketCap')
                market_cap = float(market_cap_str) if market_cap_str is not None else None

                if market_cap is not None and market_cap < CONFIG["MARKET_CAP_DELETE_THRESHOLD"]:
                    print(f"   📉 Market Cap για {data.get('baseToken_symbol', mint_address)} ({market_cap:,.2f}) κάτω από το όριο. Διαγραφή.")
                    delete_token_by_mint(mint_address)
                    tracked_mints.discard(mint_address)
                    continue

                static_data = {key: data.get(key) for key in STATIC_KEYS}
                token_id = get_or_create_token(mint_address, static_data)
                
                variable_data = {key: data.get(key) for key in VARIABLE_KEYS}
                add_metrics(token_id, variable_data)
                
                updates_made += 1
            except (ValueError, TypeError) as e:
                print(f"   ⚠️ Προειδοποίηση: Δεν ήταν δυνατή η επεξεργασία δεδομένων για {mint_address}. Σφάλμα: {e}")
            except Exception as e:
                print(f"   ❌❌ ΣΦΑΛΜΑ ΚΑΤΑ ΤΗΝ ΕΓΓΡΑΦΗ ΣΤΗ ΒΑΣΗ για το {mint_address}: {e} ❌❌")

        print(f"[PROCESSOR] ✅ Ο κύκλος ολοκληρώθηκε. Έγινε εγγραφή/έλεγχος για {len(results)} tokens.")
        
        interval = CONFIG['UPDATE_INTERVAL_SECONDS']
        print(f"[PROCESSOR] 💤 Αναμονή για {interval} δευτερόλεπτα...")
        await asyncio.sleep(interval)


# ==============================================================================
# --- 5. ΚΥΡΙΩΣ ΠΡΟΓΡΑΜΜΑ ---
# ==============================================================================

async def main():
    print("🚀 Ξεκινά το Crypto Migration Monitor & Analyzer v5 (Full-Data Edition)...")
    init_db()
    mint_queue = Queue()
    
    listener = asyncio.create_task(websocket_listener(mint_queue))
    processor = asyncio.create_task(data_processor(mint_queue))
    
    await asyncio.gather(listener, processor)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Η εκτέλεση διακόπηκε από τον χρήστη. Αντίο!")