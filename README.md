# Live Straddle Trading Dashboard

This is a real-time, event-driven options trading system built with Python, FastAPI, and ZeroMQ. It is designed for automated straddle trading, delta-neutral hedging, and provides a live dashboard for monitoring.

## Architecture

The system is composed of several microservices that communicate via ZeroMQ for high performance and low latency:

-   **Main Application (`main.py`)**: The central hub, providing the REST API, WebSocket endpoint for the UI, and managing trade lifecycle events.
-   **Market Data Service (`marketdata_service.py`)**: Connects to the broker's market data feed (XTS), processes ticks, calculates option chains with Greeks, and broadcasts data via ZMQ.
-   **Order Reconciler (`order_reconciler.py`)**: A dedicated process to poll the broker's order book, reconcile order statuses, and manage verification jobs.
-   **Snapshot Service (`snapshot_service.py`)**: Computes and broadcasts detailed, real-time PnL, Greeks, and other metrics for each active trade.
-   **Order Book Service (`order_book_service.py`)**: Caches and serves the broker's order book to other services.

## Features

-   **Microservice Architecture**: Decoupled services for scalability and resilience.
-   **Real-time Data**: WebSocket and ZeroMQ for low-latency data flow from market to UI.
-   **Event-Driven Trading**: An event bus manages trading logic (hedging, stop-loss, square-off) in a prioritized manner.
-   **Delta-Neutral Hedging**: Automated synthetic future hedging to manage position delta.
-   **Configurable Automation**: Trade parameters and monitor settings are configurable per-trade.
-   **Persistent State**: SQLite database for storing trades and orders.
-   **Shared Memory**: High-speed inter-process communication for price and order data.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd <repository-directory>
    ```

2.  **Install dependencies:**
    It's recommended to use a virtual environment.
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    pip install -r requirements.txt
    ```

3.  **Configure Credentials:**
    Copy `cred.py.example` to `cred.py` and fill in your broker API keys and secrets.
    ```python
    # cred.py
    API_KEY_I = "YOUR_INTERACTIVE_API_KEY"
    API_SECRET_I = "YOUR_INTERACTIVE_API_SECRET"
    API_KEY_M = "YOUR_MARKETDATA_API_KEY"
    API_SECRET_M = "YOUR_MARKETDATA_API_SECRET"
    clientID = "YOUR_CLIENT_ID"
    ```

4.  **Configure Settings:**
    Review `config.py` and `config.ini` for port settings and other parameters.

## Running the System

Use the provided launcher script to start all services in the correct order with health checks.

```bash
python start_all.py
```

This will start all microservices and the main application. You can then access the dashboard at `http://localhost:5000`.

To view live logs for each service, run:
```bash
python tail_all.py
```

To stop all services, press `Ctrl+C` in the terminal where `start_all.py` is running.