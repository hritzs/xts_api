"""
Verify all imports work correctly
"""

def verify_imports():
    """Test all module imports"""
    print("🔍 Verifying imports...\n")
    
    try:
        print("1. Config...")
        import config
        print("   ✅ config")
        
        print("\n2. Models...")
        from models import state, DashboardState, StraddleRequest
        print("   ✅ models.state")
        print("   ✅ models.schemas")
        
        print("\n3. Database...")
        from database import Database
        print("   ✅ database.db_manager")
        
        print("\n4. Utils...")
        from utils import logger, greeks, helpers
        print("   ✅ utils.logger")
        print("   ✅ utils.greeks")
        print("   ✅ utils.helpers")
        
        print("\n5. Market Data...")
        from market_data import socket_client, socket_callbacks, chain_provider
        print("   ✅ market_data.socket_client")
        print("   ✅ market_data.socket_callbacks")
        print("   ✅ market_data.chain_provider")
        
        print("\n6. Trading...")
        from trading import builder, order_manager, pnl_calculator
        print("   ✅ trading.builder")
        print("   ✅ trading.order_manager")
        print("   ✅ trading.pnl_calculator")
        
        print("\n7. API...")
        from api import routes, websocket
        print("   ✅ api.routes")
        print("   ✅ api.websocket")
        
        print("\n8. Background...")
        from background import tasks
        print("   ✅ background.tasks")
        
        print("\n9. External...")
        from Connect import XTSConnect
        print("   ✅ Connect")
        
        print("\n" + "=" * 60)
        print("✅ All imports successful!")
        print("=" * 60)
        
        return True
        
    except ImportError as e:
        print(f"\n❌ Import Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    verify_imports()
