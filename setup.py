"""
Setup Script - Verify Installation
"""
import sys
import subprocess


def check_dependencies():
    """Check if all required packages are installed"""
    required = [
        'fastapi',
        'uvicorn',
        'websockets',
        'python-socketio',
        'python-engineio',
        'pydantic',
        'numpy',
        'scipy',
        'requests',
        'httpx'
    ]
    
    missing = []
    
    for package in required:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing.append(package)
    
    if missing:
        print(f"❌ Missing packages: {', '.join(missing)}")
        print(f"\nInstall with: pip install {' '.join(missing)}")
        return False
    
    print("✅ All dependencies installed")
    return True


def check_structure():
    """Check if all required files exist"""
    import os
    
    required_files = [
        'main.py',
        'marketdata_service.py',
        'config.py',
        'cred.py',
        'config.ini',
        'requirements.txt',
        'Connect.py',
        'api/__init__.py',
        'api/routes.py',
        'api/websocket.py',
        'background/__init__.py',
        'background/tasks.py',
        'database/__init__.py',
        'database/db_manager.py',
        'market_data/__init__.py',
        'market_data/data_client.py',
        'market_data/socket_client.py',
        'market_data/socket_callbacks.py',
        'market_data/chain_provider.py',
        'models/__init__.py',
        'models/state.py',
        'models/schemas.py',
        'static/dashboard.html',
        'trading/__init__.py',
        'trading/builder.py',
        'trading/order_manager.py',
        'trading/pnl_calculator.py',
        'utils/__init__.py',
        'utils/greeks.py',
        'utils/helpers.py',
        'utils/logger.py'
    ]
    
    missing = []
    
    for file in required_files:
        if not os.path.exists(file):
            missing.append(file)
    
    if missing:
        print(f"❌ Missing files:")
        for file in missing:
            print(f"   - {file}")
        return False
    
    print("✅ All required files present")
    return True


def check_credentials():
    """Check if credentials are configured"""
    try:
        import cred
        
        if not hasattr(cred, 'API_KEY_I') or not cred.API_KEY_I:
            print("❌ API_KEY_I not configured in cred.py")
            return False
        
        if not hasattr(cred, 'API_SECRET_I') or not cred.API_SECRET_I:
            print("❌ API_SECRET_I not configured in cred.py")
            return False
        
        if not hasattr(cred, 'API_KEY_M') or not cred.API_KEY_M:
            print("❌ API_KEY_M not configured in cred.py")
            return False
        
        if not hasattr(cred, 'API_SECRET_M') or not cred.API_SECRET_M:
            print("❌ API_SECRET_M not configured in cred.py")
            return False
        
        print("✅ Credentials configured")
        return True
        
    except ImportError:
        print("❌ cred.py not found")
        return False


def main():
    """Run all checks"""
    print("=" * 60)
    print("🔍 Straddle Trading Dashboard - Setup Verification")
    print("=" * 60)
    print()
    
    checks = [
        ("Dependencies", check_dependencies),
        ("File Structure", check_structure),
        ("Credentials", check_credentials)
    ]
    
    all_passed = True
    
    for name, check_func in checks:
        print(f"\n📋 Checking {name}...")
        if not check_func():
            all_passed = False
        print()
    
    print("=" * 60)
    if all_passed:
        print("✅ All checks passed! Ready to run.")
        print("\nStart the dashboard with:")
        print("   python main.py")
    else:
        print("❌ Some checks failed. Please fix the issues above.")
    print("=" * 60)


if __name__ == "__main__":
    main()
