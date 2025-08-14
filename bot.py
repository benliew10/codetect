import asyncio
import logging
import os
from typing import List, Set
from io import BytesIO

from telegram import Update
from telegram.constants import ChatType, ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from storage import Storage


logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("code-distributor-bot")


def _parse_admin_ids(raw: str) -> Set[int]:
	ids: Set[int] = set()
	for chunk in raw.split(","):
		chunk = chunk.strip()
		if not chunk:
			continue
		try:
			ids.add(int(chunk))
		except ValueError:
			logger.warning("Ignoring invalid admin id: %s", chunk)
	return ids


BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_IDS: Set[int] = _parse_admin_ids(os.environ.get("ADMIN_IDS", ""))
DB_PATH = os.environ.get("DB_PATH", "codes.db").strip() or "codes.db"

if not BOT_TOKEN:
	raise RuntimeError("Missing TELEGRAM_BOT_TOKEN environment variable")
if not ADMIN_IDS:
	raise RuntimeError("ADMIN_IDS is empty. Provide comma-separated Telegram user IDs of admins.")


storage = Storage(db_path=DB_PATH)


def _is_admin(user_id: int) -> bool:
	return user_id in ADMIN_IDS


def _extract_codes_from_text(text: str) -> List[str]:
	# Normalize newlines
	text = text.replace("\r\n", "\n").replace("\r", "\n")
	results: List[str] = []
	seen = set()
	for raw_line in text.split("\n"):
		line = raw_line.strip()
		if not line:
			continue
		# Support comma-separated values on a single line as well
		parts = [p.strip() for p in line.split(",")]
		for p in parts:
			if not p:
				continue
			if p not in seen:
				seen.add(p)
				results.append(p)
	return results


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	user = update.effective_user
	if update.effective_chat.type == ChatType.PRIVATE:
		await update.effective_chat.send_message(
			"Send me text or a .txt file with one code per line to upload.\n"
			"In a group, use /distribute to post one unused code."
		)
	else:
		await update.effective_chat.send_message(
			"Hello! Admins can use /distribute here to post a code."
		)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	await update.effective_chat.send_message(
		"Commands:\n"
		"/distribute - Post one unused code to the current chat (admins only)\n"
		"/remaining - Show how many unused codes are left (admins only)\n"
		"/usage - Show how many codes each admin has distributed (admins only)\n"
		"/upload - Reply with text or .txt file to batch upload (admins only)\n"
		"Upload codes by DM'ing this bot with text or a .txt file (admins only)."
	)


async def handle_private_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	message = update.effective_message
	user = update.effective_user
	if user is None or message is None:
		return
	if not _is_admin(user.id):
		await update.effective_chat.send_message("Not authorized.")
		return

	codes: List[str] = []
	# Text upload
	if message.text:
		codes = _extract_codes_from_text(message.text)
	# Text document upload (.txt or text/*)
	elif message.document:
		doc = message.document
		is_text_mime = (doc.mime_type or "").startswith("text/")
		is_txt_ext = (doc.file_name or "").lower().endswith(".txt")
		if not (is_text_mime or is_txt_ext):
			await update.effective_chat.send_message("Please upload a .txt file or send plain text.")
			return
		file = await doc.get_file()
		buffer = BytesIO()
		await file.download_to_memory(out=buffer)
		data = buffer.getvalue()
		try:
			text = data.decode("utf-8")
		except UnicodeDecodeError:
			text = data.decode("latin-1", errors="ignore")
		codes = _extract_codes_from_text(text)
	else:
		await update.effective_chat.send_message("Send text or a .txt file with codes (one per line).")
		return

	if not codes:
		await update.effective_chat.send_message("No codes found in your message.")
		return

	inserted, duplicates = await storage.insert_codes(codes=codes, uploaded_by=user.id)
	remaining = await storage.count_unused()
	await update.effective_chat.send_message(
		f"Uploaded: {inserted} new | Duplicates ignored: {duplicates} | Unused total: {remaining}"
	)


async def cmd_distribute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	user = update.effective_user
	if user is None or not _is_admin(user.id):
		await update.effective_chat.send_message("Not authorized.")
		return

	code_value = await storage.get_and_mark_next_unused(used_by=user.id)
	if code_value is None:
		await update.effective_chat.send_message("No unused codes remaining.")
		return

	# Render the code in monospaced formatting
	await update.effective_chat.send_message(
		f"Distributed code:\n<code>{code_value}</code>",
		parse_mode=ParseMode.HTML,
		disable_web_page_preview=True,
	)


async def cmd_remaining(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	user = update.effective_user
	if user is None or not _is_admin(user.id):
		await update.effective_chat.send_message("Not authorized.")
		return
	remaining = await storage.count_unused()
	await update.effective_chat.send_message(f"Unused codes left: {remaining}")


async def cmd_usage(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	user = update.effective_user
	if user is None or not _is_admin(user.id):
		await update.effective_chat.send_message("Not authorized.")
		return
	counts = await storage.usage_counts()
	if not counts:
		await update.effective_chat.send_message("No codes distributed yet.")
		return
	lines = ["Usage per user (user_id: count):"]
	for uid, cnt in counts:
		lines.append(f"- {uid}: {cnt}")
	await update.effective_chat.send_message("\n".join(lines))


async def cmd_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	"""Admin-only: Reply to a text or .txt document to batch upload in private chat."""
	user = update.effective_user
	chat = update.effective_chat
	if user is None or chat is None or not _is_admin(user.id) or chat.type != ChatType.PRIVATE:
		await update.effective_chat.send_message("Not authorized or wrong context. Use this in a private chat.")
		return

	if not update.effective_message or not update.effective_message.reply_to_message:
		await update.effective_chat.send_message("Reply to a text or .txt message with /upload.")
		return

	msg = update.effective_message.reply_to_message
	codes: List[str] = []
	if msg.text and not msg.text.startswith("/"):
		codes = _extract_codes_from_text(msg.text)
	elif msg.document:
		doc = msg.document
		is_text_mime = (doc.mime_type or "").startswith("text/")
		is_txt_ext = (doc.file_name or "").lower().endswith(".txt")
		if not (is_text_mime or is_txt_ext):
			await update.effective_chat.send_message("Please reply to a .txt file or plain text message.")
			return
		file = await doc.get_file()
		buffer = BytesIO()
		await file.download_to_memory(out=buffer)
		data = buffer.getvalue()
		try:
			text = data.decode("utf-8")
		except UnicodeDecodeError:
			text = data.decode("latin-1", errors="ignore")
		codes = _extract_codes_from_text(text)
	else:
		await update.effective_chat.send_message("Unsupported message type. Reply to text or .txt file.")
		return

	if not codes:
		await update.effective_chat.send_message("No codes found in the replied message.")
		return

	inserted, duplicates = await storage.insert_codes(codes=codes, uploaded_by=user.id)
	remaining = await storage.count_unused()
	await update.effective_chat.send_message(
		f"Uploaded: {inserted} new | Duplicates ignored: {duplicates} | Unused total: {remaining}"
	)


def main() -> None:
	# Initialize storage (async) before starting the bot
	asyncio.run(storage.initialize())

	app = Application.builder().token(BOT_TOKEN).build()

	# Commands
	app.add_handler(CommandHandler("start", cmd_start))
	app.add_handler(CommandHandler("help", cmd_help))
	app.add_handler(CommandHandler("distribute", cmd_distribute, filters=filters.ChatType.GROUPS))
	app.add_handler(CommandHandler("remaining", cmd_remaining))
	app.add_handler(CommandHandler("usage", cmd_usage))
	app.add_handler(CommandHandler("upload", cmd_upload))

	# Private chat uploads: text or .txt documents
	private_text_filter = filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND
	private_doc_filter = (
		filters.ChatType.PRIVATE
		& (filters.Document.MimeType("text/plain") | filters.Document.FileExtension("txt"))
	)
	app.add_handler(MessageHandler(private_text_filter, handle_private_upload))
	app.add_handler(MessageHandler(private_doc_filter, handle_private_upload))

	logger.info("Bot is starting with polling...")
	app.run_polling()


if __name__ == "__main__":
	main()


