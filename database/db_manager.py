"""
Database Manager - SQLite Operations with Config Support
"""
import sqlite3
import json
import threading
from typing import Dict, List, Optional
from utils.logger import logger
from utils.helpers import get_ist_date_str
import config


class Database:
    """SQLite Database Manager with thread-safe operations"""
    
    def __init__(self, db_name: str = config.DATABASE_NAME):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self.lock = threading.Lock()
        self.init_db()
    
    def init_db(self):
        """Initialize database tables"""
        try:
            self.conn = sqlite3.connect(self.db_name, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row  # Enable dict-like access
            self.cursor = self.conn.cursor()
            
            # ══════════════════════════════════════════════════════════════
            # ORDERS TABLE
            # ══════════════════════════════════════════════════════════════
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    app_order_id TEXT UNIQUE,
                    order_unique_id TEXT,
                    exchange_order_id TEXT,
                    symbol TEXT,
                    trading_symbol TEXT,
                    exchange_segment TEXT,
                    exchange_instrument_id INTEGER,
                    order_side TEXT,
                    order_type TEXT,
                    product_type TEXT,
                    order_quantity INTEGER,
                    order_price REAL,
                    order_stop_price REAL,
                    order_status TEXT,
                    order_avg_price REAL,
                    cumulative_quantity INTEGER,
                    leaves_quantity INTEGER,
                    cancel_reject_reason TEXT,
                    order_generated_datetime TEXT,
                    exchange_transact_time TEXT,
                    last_update_datetime TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_date TEXT
                )
            """)
            
            # ══════════════════════════════════════════════════════════════
            # STRADDLES TABLE - UPDATED with Delta-Neutral & Config
            # ══════════════════════════════════════════════════════════════
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS straddles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    straddle_id TEXT UNIQUE,
                    trade_uid TEXT,
                    symbol TEXT,
                    strike INTEGER,
                    expiry TEXT,
                    
                    -- Lots and Quantities (Delta-Neutral Support)
                    pe_lots INTEGER,
                    ce_lots INTEGER,
                    lots INTEGER,
                    pe_quantity INTEGER,
                    ce_quantity INTEGER,
                    quantity INTEGER,
                    total_quantity INTEGER,
                    
                    -- CE Details
                    ce_token INTEGER,
                    ce_symbol TEXT,
                    ce_order_id TEXT,
                    ce_app_order_id TEXT,
                    ce_entry_price REAL,
                    ce_delta REAL,
                    
                    -- PE Details
                    pe_token INTEGER,
                    pe_symbol TEXT,
                    pe_order_id TEXT,
                    pe_app_order_id TEXT,
                    pe_entry_price REAL,
                    pe_delta REAL,
                    
                    -- Greeks & Delta-Neutral
                    net_delta REAL,
                    delta_neutral INTEGER DEFAULT 0,
                    
                    -- Premium & Status
                    total_premium REAL,
                    status TEXT DEFAULT 'PENDING',
                    execution_time REAL,
                    
                    -- Config-Based Trading
                    config TEXT,
                    entry_spot REAL,
                    sl_points REAL,
                    
                    -- Timestamps
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    closed_at TIMESTAMP,
                    created_date TEXT
                )
            """)
            
            # ══════════════════════════════════════════════════════════════
            # MIGRATION: Add missing columns if they don't exist
            # ══════════════════════════════════════════════════════════════
            self._migrate_database()
            
            self.conn.commit()
            logger.info("✅ Database initialized")
            
        except Exception as e:
            logger.error(f"❌ Database init error: {e}")
            raise
    
    def _migrate_database(self):
        """Add missing columns to existing tables"""
        try:
            # Check if trade_uid column exists
            self.cursor.execute("PRAGMA table_info(straddles)")
            columns = [col[1] for col in self.cursor.fetchall()]
            
            # Add trade_uid if missing
            if 'trade_uid' not in columns:
                logger.info("🔄 Migrating database: Adding trade_uid column")
                self.cursor.execute("ALTER TABLE straddles ADD COLUMN trade_uid TEXT")
                self.conn.commit()
                logger.info("✅ Migration complete: trade_uid added")
            
            # Add ce_app_order_id if missing
            if 'ce_app_order_id' not in columns:
                logger.info("🔄 Migrating database: Adding ce_app_order_id column")
                self.cursor.execute("ALTER TABLE straddles ADD COLUMN ce_app_order_id TEXT")
                self.conn.commit()
                logger.info("✅ Migration complete: ce_app_order_id added")
            
            # Add pe_app_order_id if missing
            if 'pe_app_order_id' not in columns:
                logger.info("🔄 Migrating database: Adding pe_app_order_id column")
                self.cursor.execute("ALTER TABLE straddles ADD COLUMN pe_app_order_id TEXT")
                self.conn.commit()
                logger.info("✅ Migration complete: pe_app_order_id added")
            
            # Add ce_gamma if missing
            if 'ce_gamma' not in columns:
                logger.info("🔄 Migrating database: Adding ce_gamma column")
                self.cursor.execute("ALTER TABLE straddles ADD COLUMN ce_gamma REAL DEFAULT 0")
                self.conn.commit()
                logger.info("✅ Migration complete: ce_gamma added")
            
            # Add pe_gamma if missing
            if 'pe_gamma' not in columns:
                logger.info("🔄 Migrating database: Adding pe_gamma column")
                self.cursor.execute("ALTER TABLE straddles ADD COLUMN pe_gamma REAL DEFAULT 0")
                self.conn.commit()
                logger.info("✅ Migration complete: pe_gamma added")
            
            # Add ce_theta if missing
            if 'ce_theta' not in columns:
                logger.info("🔄 Migrating database: Adding ce_theta column")
                self.cursor.execute("ALTER TABLE straddles ADD COLUMN ce_theta REAL DEFAULT 0")
                self.conn.commit()
                logger.info("✅ Migration complete: ce_theta added")
            
            # Add pe_theta if missing
            if 'pe_theta' not in columns:
                logger.info("🔄 Migrating database: Adding pe_theta column")
                self.cursor.execute("ALTER TABLE straddles ADD COLUMN pe_theta REAL DEFAULT 0")
                self.conn.commit()
                logger.info("✅ Migration complete: pe_theta added")

            # Add entry_timestamp if missing
            if 'entry_timestamp' not in columns:
                logger.info("🔄 Migrating database: Adding entry_timestamp column")
                self.cursor.execute("ALTER TABLE straddles ADD COLUMN entry_timestamp TEXT")
                self.conn.commit()
                logger.info("✅ Migration complete: entry_timestamp added")
            
        except Exception as e:
            logger.error(f"❌ Migration error: {e}")
            # Don't raise, continue with existing schema
    
    def insert_order(self, order_data: Dict):
        """Insert/update order"""
        with self.lock:
            try:
                self.cursor.execute("""
                    INSERT OR REPLACE INTO orders (
                        app_order_id, order_unique_id, exchange_order_id, symbol,
                        trading_symbol, exchange_segment, exchange_instrument_id,
                        order_side, order_type, product_type, order_quantity,
                        order_price, order_stop_price, order_status, order_avg_price,
                        cumulative_quantity, leaves_quantity, cancel_reject_reason,
                        order_generated_datetime, exchange_transact_time, last_update_datetime,
                        created_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(order_data.get('AppOrderID')),
                    order_data.get('OrderUniqueIdentifier'),
                    order_data.get('ExchangeOrderID'),
                    order_data.get('Symbol', ''),
                    order_data.get('TradingSymbol', ''),
                    order_data.get('ExchangeSegment'),
                    order_data.get('ExchangeInstrumentID'),
                    order_data.get('OrderSide'),
                    order_data.get('OrderType'),
                    order_data.get('ProductType'),
                    order_data.get('OrderQuantity'),
                    order_data.get('OrderPrice', 0),
                    order_data.get('OrderStopPrice', 0),
                    order_data.get('OrderStatus'),
                    order_data.get('OrderAverageTradedPrice', 0),
                    order_data.get('CumulativeQuantity', 0),
                    order_data.get('LeavesQuantity', 0),
                    order_data.get('CancelRejectReason', ''),
                    order_data.get('OrderGeneratedDateTime'),
                    order_data.get('ExchangeTransactTime'),
                    order_data.get('LastUpdateDateTime'),
                    get_ist_date_str()
                ))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Insert order error: {e}")
    
    def insert_straddle(self, straddle_data: Dict):
        """Insert straddle with delta-neutral support"""
        with self.lock:
            try:
                # --- NEW: Handle config object ---
                config_json = None
                if 'config' in straddle_data and isinstance(straddle_data['config'], dict):
                    config_json = json.dumps(straddle_data['config'])

                # Count the values to ensure they match
                values = (
                    straddle_data['straddle_id'],
                    straddle_data.get('trade_uid', straddle_data['straddle_id']),
                    straddle_data['symbol'],
                    straddle_data['strike'],
                    straddle_data.get('expiry', ''),
                    straddle_data.get('pe_lots', straddle_data.get('lots', 1)),
                    straddle_data.get('ce_lots', straddle_data.get('lots', 1)),
                    straddle_data.get('lots', 1),
                    straddle_data.get('pe_quantity', straddle_data.get('quantity', 0)),
                    straddle_data.get('ce_quantity', straddle_data.get('quantity', 0)),
                    straddle_data.get('quantity', 0),
                    straddle_data.get('total_quantity', straddle_data.get('quantity', 0)),
                    straddle_data['ce_token'],
                    straddle_data['ce_symbol'],
                    straddle_data.get('ce_order_id', ''),
                    straddle_data.get('ce_app_order_id', ''),
                    straddle_data.get('ce_entry_price', 0),
                    straddle_data.get('ce_delta', 0),
                    straddle_data.get('ce_gamma', 0),
                    straddle_data.get('ce_theta', 0),
                    straddle_data['pe_token'],
                    straddle_data['pe_symbol'],
                    straddle_data.get('pe_order_id', ''),
                    straddle_data.get('pe_app_order_id', ''),
                    straddle_data.get('pe_entry_price', 0),
                    straddle_data.get('pe_delta', 0),
                    straddle_data.get('pe_gamma', 0),
                    straddle_data.get('pe_theta', 0),
                    straddle_data.get('net_delta', 0),
                    1 if straddle_data.get('delta_neutral', False) else 0,
                    straddle_data.get('total_premium', 0),
                    straddle_data.get('status', 'ACTIVE'),
                    straddle_data.get('execution_time', 0),
                    straddle_data.get('entry_spot', 0),
                    config_json,  # Pass config as JSON
                    straddle_data.get('sl_points', 0.0),
                    straddle_data.get('entry_timestamp', ''),
                    get_ist_date_str()
                )

                # Debug: check lengths
                columns = ['straddle_id', 'trade_uid', 'symbol', 'strike', 'expiry', 'pe_lots', 'ce_lots', 'lots', 'pe_quantity', 'ce_quantity', 'quantity', 'total_quantity', 'ce_token', 'ce_symbol', 'ce_order_id', 'ce_app_order_id', 'ce_entry_price', 'ce_delta', 'ce_gamma', 'ce_theta', 'pe_token', 'pe_symbol', 'pe_order_id', 'pe_app_order_id', 'pe_entry_price', 'pe_delta', 'pe_gamma', 'pe_theta', 'net_delta', 'delta_neutral', 'total_premium', 'status', 'execution_time', 'entry_spot', 'config', 'sl_points', 'entry_timestamp', 'created_date']
                logger.debug(f"Columns: {len(columns)}, Values: {len(values)}")
                
                self.cursor.execute(f"""
                    INSERT OR REPLACE INTO straddles (
                        {','.join(columns)}
                    ) VALUES ({','.join(['?'] * len(columns))})
                """, values)
                self.conn.commit()
                logger.info(f"💾 Straddle saved: {straddle_data['straddle_id']}")
            except Exception as e:
                logger.error(f"Insert straddle error: {e}")
                import traceback
                logger.error(traceback.format_exc())
    
    def update_straddle_entry_prices(self, straddle_id: str, ce_price: float, pe_price: float):
        """Update straddle with fill prices"""
        with self.lock:
            try:
                self.cursor.execute("""
                    UPDATE straddles 
                    SET ce_entry_price = ?, pe_entry_price = ?, 
                        total_premium = (? + ?) * quantity,
                        status = 'FILLED'
                    WHERE straddle_id = ? OR trade_uid = ?
                """, (ce_price, pe_price, ce_price, pe_price, straddle_id, straddle_id))
                self.conn.commit()
                logger.info(f"✅ {straddle_id}: CE=₹{ce_price}, PE=₹{pe_price}")
            except Exception as e:
                logger.error(f"Update error: {e}")
    
    def update_straddle_config(self, straddle_id: str, config: Dict, sl_points: float):
        """Store config with straddle"""
        with self.lock:
            try:
                self.cursor.execute("""
                    UPDATE straddles 
                    SET config = ?, sl_points = ?
                    WHERE straddle_id = ? OR trade_uid = ?
                """, (json.dumps(config), sl_points, straddle_id, straddle_id))
                
                self.conn.commit()
                logger.info(f"✅ Config saved: {straddle_id}")
                
            except Exception as e:
                logger.error(f"❌ Save config error: {e}")
    
    def update_straddle_status(self, straddle_id: str, status: str):
        """Update straddle status"""
        with self.lock:
            try:
                self.cursor.execute("""
                    UPDATE straddles 
                    SET status = ?,
                        closed_at = CASE WHEN ? LIKE 'CLOSED%' THEN CURRENT_TIMESTAMP ELSE closed_at END
                    WHERE straddle_id = ? OR trade_uid = ?
                """, (status, status, straddle_id, straddle_id))
                
                self.conn.commit()
                logger.info(f"✅ Status updated: {straddle_id} -> {status}")
                
            except Exception as e:
                logger.error(f"❌ Update status error: {e}")
    
    def update_straddle_strike(self, trade_uid: str, new_strike: int):
        """Update the strike price of a straddle after a roll."""
        with self.lock:
            try:
                self.cursor.execute("""
                    UPDATE straddles 
                    SET strike = ?
                    WHERE trade_uid = ? OR straddle_id = ?
                """, (new_strike, trade_uid, trade_uid))
                self.conn.commit()
                logger.info(f"✅ Strike updated for {trade_uid} -> {new_strike}")
            except Exception as e:
                logger.error(f"❌ Update strike error for {trade_uid}: {e}")

    def get_straddle_by_id(self, straddle_id: str) -> Optional[Dict]:
        """Get straddle by ID"""
        with self.lock:
            try:
                self.cursor.execute("""
                    SELECT * FROM straddles WHERE straddle_id = ? OR trade_uid = ?
                """, (straddle_id, straddle_id))
                
                row = self.cursor.fetchone()
                if row:
                    result = dict(row)
                    # Parse config JSON if present
                    if result.get('config'):
                        try:
                            result['config'] = json.loads(result['config'])
                        except:
                            pass
                    return result
                return None
                
            except Exception as e:
                logger.error(f"❌ Get straddle error: {e}")
                return None
    
    def get_orders_by_trade_id(self, trade_uid: str) -> List[Dict]:
        """
        Fetches all orders from the database for a specific trade UID.
        This is a more efficient replacement for fetching all orders and filtering in Python.
        """
        with self.lock:
            try:
                # The trade_uid is part of the order_unique_id (e.g., BUILD_ny1234_... or HEDGE_ny1234_...)
                # FIX: Also check for truncated UID if the full UID search fails or just use a broader search
                # If trade_uid is 'ny040326150700a', we search for '%ny040326150700a%'.
                # If that fails, we could try searching for the base without the suffix if needed, but fixing insertion is better.
                self.cursor.execute("SELECT * FROM orders WHERE order_unique_id LIKE ?", (f'%{trade_uid}%',))
                rows = self.cursor.fetchall()
                orders = [dict(row) for row in rows]
                
                # --- FIX: Fallback for suffixed trade UIDs matching base order UIDs ---
                if not orders and trade_uid and trade_uid[-1].isalpha() and len(trade_uid) > 1 and trade_uid[-2].isdigit():
                    base_uid = trade_uid[:-1]
                    logger.warning(f"⚠️ No orders found for {trade_uid}. Trying fallback search with base UID {base_uid}...")
                    self.cursor.execute("SELECT * FROM orders WHERE order_unique_id LIKE ?", (f'%{base_uid}%',))
                    rows = self.cursor.fetchall()
                    fallback_orders = [dict(row) for row in rows]
                    if fallback_orders:
                        logger.info(f"✅ Found {len(fallback_orders)} orders using base UID {base_uid}.")
                        orders = fallback_orders

                logger.info(f"🔍 Found {len(orders)} orders in DB for trade {trade_uid} via get_orders_by_trade_id.")
                return orders
            except Exception as e:
                logger.error(f"❌ Database error in get_orders_by_trade_id for {trade_uid}: {e}")
                return []

    def get_todays_orders(self) -> List[Dict]:
        """Get today's orders"""
        with self.lock:
            try:
                today = get_ist_date_str()
                self.cursor.execute("""
                    SELECT * FROM orders 
                    WHERE created_date = ? 
                    ORDER BY created_at DESC
                """, (today,))
                rows = self.cursor.fetchall()
                return [dict(row) for row in rows]
            except Exception as e:
                logger.error(f"Get orders error: {e}")
                return []
    
    def get_todays_straddles(self) -> List[Dict]:
        """Get today's straddles"""
        with self.lock:
            try:
                today = get_ist_date_str()
                self.cursor.execute("""
                    SELECT * FROM straddles 
                    WHERE created_date = ? 
                    ORDER BY created_at DESC
                """, (today,))
                rows = self.cursor.fetchall()
                result = []
                for row in rows:
                    straddle = dict(row)
                    # Parse config JSON if present
                    if straddle.get('config'):
                        try:
                            straddle['config'] = json.loads(straddle['config'])
                        except:
                            pass
                    result.append(straddle)
                return result
            except Exception as e:
                logger.error(f"Get straddles error: {e}")
                return []
    
    def get_active_straddles(self) -> List[Dict]:
        """Get active straddles"""
        with self.lock:
            try:
                today = get_ist_date_str()
                self.cursor.execute("""
                    SELECT * FROM straddles 
                    WHERE status IN ('FILLED', 'ACTIVE', 'PENDING') 
                    AND created_date = ?
                    ORDER BY created_at DESC
                """, (today,))
                rows = self.cursor.fetchall()
                result = []
                for row in rows:
                    straddle = dict(row)
                    # Parse config JSON if present
                    if straddle.get('config'):
                        try:
                            straddle['config'] = json.loads(straddle['config'])
                        except:
                            pass
                    result.append(straddle)
                return result
            except Exception as e:
                logger.error(f"Get active straddles error: {e}")
                return []
    
    def get_all_straddles(self) -> List[Dict]:
        """Get all straddles (for debugging)"""
        with self.lock:
            try:
                self.cursor.execute("""
                    SELECT * FROM straddles 
                    ORDER BY created_at DESC
                """)
                rows = self.cursor.fetchall()
                result = []
                for row in rows:
                    straddle = dict(row)
                    if straddle.get('config'):
                        try:
                            straddle['config'] = json.loads(straddle['config'])
                        except:
                            pass
                    result.append(straddle)
                return result
            except Exception as e:
                logger.error(f"Get all straddles error: {e}")
                return []
    
    def insert_orders_bulk(self, orders_data: List[Dict]):
        """Insert/update a batch of orders in a single transaction."""
        if not orders_data:
            return

        with self.lock:
            try:
                # Prepare data for executemany
                records_to_insert = []
                for order_data in orders_data:
                    record = (
                        str(order_data.get('AppOrderID')),
                        order_data.get('OrderUniqueIdentifier'),
                        order_data.get('ExchangeOrderID'),
                        order_data.get('Symbol', ''),
                        order_data.get('TradingSymbol', ''),
                        order_data.get('ExchangeSegment'),
                        order_data.get('ExchangeInstrumentID'),
                        order_data.get('OrderSide'),
                        order_data.get('OrderType'),
                        order_data.get('ProductType'),
                        order_data.get('OrderQuantity'),
                        order_data.get('OrderPrice', 0),
                        order_data.get('OrderStopPrice', 0),
                        order_data.get('OrderStatus'),
                        order_data.get('OrderAverageTradedPrice', 0),
                        order_data.get('CumulativeQuantity', 0),
                        order_data.get('LeavesQuantity', 0),
                        order_data.get('CancelRejectReason', ''),
                        order_data.get('OrderGeneratedDateTime'),
                        order_data.get('ExchangeTransactTime'),
                        order_data.get('LastUpdateDateTime'),
                        get_ist_date_str()
                    )
                    records_to_insert.append(record)

                # Use a single transaction for bulk insert/replace
                self.cursor.execute("BEGIN TRANSACTION")
                self.cursor.executemany("""
                    INSERT OR REPLACE INTO orders (app_order_id, order_unique_id, exchange_order_id, symbol, trading_symbol, exchange_segment, exchange_instrument_id, order_side, order_type, product_type, order_quantity, order_price, order_stop_price, order_status, order_avg_price, cumulative_quantity, leaves_quantity, cancel_reject_reason, order_generated_datetime, exchange_transact_time, last_update_datetime, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, records_to_insert)
                self.conn.commit()
                logger.info(f"✅ Bulk inserted/updated {len(records_to_insert)} orders.")
            except Exception as e:
                logger.error(f"Bulk insert orders error: {e}")
                self.conn.rollback() # Rollback on error

    def close(self):
        """Close database"""
        if self.conn:
            self.conn.close()
            logger.info("🔒 Database closed")
