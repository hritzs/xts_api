from flask import Flask, render_template, request, redirect, url_for, flash, session
import requests
import json

app = Flask(__name__)
# IMPORTANT: Change this to a random, secret string in a real application
app.secret_key = 'a-very-secret-and-random-key'

# The API URL for getting a session token from the Postman collection
SESSION_TOKEN_URL = "http://greekapi.greeksoft.in:3001/auth/greek/sessiontoken"

def get_session_token(username, password):
    """
    Authenticates with the Greek REST API and retrieves a session token.

    Args:
        username (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        str: The session token if authentication is successful, otherwise None.
    """
    payload = {
        "username": username,
        "password": password,
        "validFor": "30d"
    }
    headers = {
        "Content-Type": "application/json"
    }

    try:
        # Set a timeout for the request
        response = requests.post(SESSION_TOKEN_URL, headers=headers, data=json.dumps(payload), timeout=10)

        # The Postman collection test expects a 201 status code for success
        if response.status_code == 201:
            response_data = response.json()
            return response_data.get("sessionToken")
        else:
            # Log the error for debugging purposes
            print(f"API Authentication Error: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Network or Request Error: {e}")
        return None

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        token = get_session_token(username, password)

        if token:
            # Store token and user info in the session
            session['session_token'] = token
            session['username'] = username
            flash('Login successful!', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid credentials or API error. Please try again.', 'danger')

    return render_template('login.html')

@app.route('/dashboard')
def dashboard():
    if 'session_token' not in session:
        flash('You must be logged in to view this page.', 'warning')
        return redirect(url_for('login'))

    return render_template('dashboard.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))

@app.route('/')
def index():
    return redirect(url_for('login'))

if __name__ == '__main__':
    # For development only. Use a production-ready WSGI server like Gunicorn for deployment.
    app.run(host='0.0.0.0', port=5001, debug=True)

