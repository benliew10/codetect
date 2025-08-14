### Telegram Code Distributor Bot

**Purpose**: Admin privately uploads codes (text or .txt). In a group chat, admin runs a command to distribute one unused code at a time. Each code is marked used immediately and will not be sent again.

### Requirements

- Python 3.10+
- A Telegram Bot token from BotFather

### Setup

1. Create a bot with BotFather and obtain the token.
2. Get your Telegram numeric user ID (e.g., via `@userinfobot`).
3. In your shell, export environment variables:

```bash
export TELEGRAM_BOT_TOKEN="YOUR_BOT_TOKEN"
export ADMIN_IDS="123456789,987654321"  # comma-separated admin user IDs
```

4. Install dependencies and run:

```bash
cd "/Users/Apple/Desktop/text"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python bot.py
```

### Usage

- Add the bot to your target group chat.
- In a private chat with the bot (admins only):
  - Send plain text with one code per line, or
  - Upload a `.txt` file (one code per line). Comma-separated values in a line are also accepted.
- In the group chat (admins only):
  - Run `/distribute` to post one unused code.
  - Run `/remaining` to see how many unused codes are left.
  - Run `/usage` to see per-admin distribution counts.
  
### Advanced (Batch Upload via Command)

- In private chat, reply to a text or `.txt` message and send `/upload` to batch import. This avoids mixing commands with code content.

### Notes

- Codes are stored in `codes.db` (SQLite) in the project folder.
- Duplicate codes (already stored) are ignored automatically.
- Distribution is FIFO based on upload order and is safe against concurrent triggers.

### Deploy on Render

1. Push this folder to a Git repo (GitHub/GitLab).
2. On Render, create a new Blueprint from your repo and point to `render.yaml` in the root.
3. Set environment variables on the service:
   - `TELEGRAM_BOT_TOKEN` (required)
   - `ADMIN_IDS` (required, comma-separated)
   - `DB_PATH` (optional; defaults to `/var/data/codes.db` via `render.yaml`)
4. Render will provision a worker with a 1GB persistent disk at `/var/data` to keep your SQLite DB across deploys.
5. Start the service. The bot will run as a background Worker and use long polling.


