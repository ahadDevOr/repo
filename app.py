import threading
import asyncio
import os
from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!", 200

@app.route('/health')
def health():
    return "OK", 200

def run_bot():
    from main import main
    main()

if __name__ == '__main__':
    # Bot কে background thread এ চালাও
    t = threading.Thread(target=run_bot, daemon=True)
    t.start()
    # Flask চালাও
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
