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
			"发送文本或 .txt 文件（每行一个兑换码）来上传。\n"
			"在群组中，管理员可发送‘发码’发布一个未使用的兑换码。"
		)
	else:
		await update.effective_chat.send_message("你好！管理员可以发送‘发码’在这里发送兑换码。")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	await update.effective_chat.send_message(
		"指令（中文触发与兼容斜杠）：\n"
		"发码 /fa - 在当前群发送一个未使用的兑换码（仅管理员）\n"
		"余量 /yuliang - 显示剩余未使用兑换码数量（仅管理员）\n"
		"用量 /yongliang - 显示各管理员今日与累计发放数量（仅管理员）\n"
		"上传 /shangchuan - 私聊中，回复文本或 .txt 文件进行批量上传（仅管理员）\n"
		"重置 /chongzhi - 私聊中，清空所有兑换码（仅管理员）\n"
		"提示：若要在群里使用中文触发词，需在 BotFather 将隐私模式关闭。"
	)


async def handle_private_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	message = update.effective_message
	user = update.effective_user
	if user is None or message is None:
		return
	if not _is_admin(user.id):
		await update.effective_chat.send_message("无权限。")
		return

	# Record/refresh admin display name & username
	await storage.upsert_user(user_id=user.id, display_name=getattr(user, "full_name", user.first_name or ""), username=user.username)

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
			await update.effective_chat.send_message("请上传 .txt 文件或发送纯文本。")
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
		await update.effective_chat.send_message("发送文本或 .txt 文件（每行一个兑换码）。")
		return

	if not codes:
		await update.effective_chat.send_message("未在消息中找到兑换码。")
		return

	inserted, duplicates = await storage.insert_codes(codes=codes, uploaded_by=user.id)
	remaining = await storage.count_unused()
	await update.effective_chat.send_message(
		f"上传成功：新增 {inserted} 条｜忽略重复 {duplicates} 条｜剩余未使用 {remaining} 条"
	)


async def cmd_distribute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	user = update.effective_user
	if user is None or not _is_admin(user.id):
		await update.effective_chat.send_message("无权限。")
		return

	# Record/refresh admin display name & username
	await storage.upsert_user(user_id=user.id, display_name=getattr(user, "full_name", user.first_name or ""), username=user.username)

	code_value = await storage.get_and_mark_next_unused(used_by=user.id)
	if code_value is None:
		await update.effective_chat.send_message("没有可用的兑换码，请先上传。")
		return

	# Render the code in monospaced formatting
	await update.effective_chat.send_message(
		f"兑换码：\n<code>{code_value}</code>",
		parse_mode=ParseMode.HTML,
		disable_web_page_preview=True,
	)


async def cmd_remaining(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	user = update.effective_user
	if user is None or not _is_admin(user.id):
		await update.effective_chat.send_message("无权限。")
		return
	remaining = await storage.count_unused()
	await update.effective_chat.send_message(f"剩余未使用：{remaining} 条")


async def cmd_usage(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	user = update.effective_user
	if user is None or not _is_admin(user.id):
		await update.effective_chat.send_message("无权限。")
		return
	counts_total = await storage.usage_counts_with_names()
	counts_today = await storage.usage_counts_with_names_today()
	grand_total = await storage.total_used_count()
	grand_today = await storage.total_used_today()

	lines = ["发放统计："]
	lines.append(f"- 今日总计：{grand_today}")
	lines.append(f"- 累计总计：{grand_total}")
	lines.append("")
	lines.append("今日各管理员：")
	if counts_today:
		for uid, display_name, username, cnt in counts_today:
			name = display_name or (f"@{username}" if username else str(uid))
			if username and display_name:
				name = f"{display_name} (@{username})"
			lines.append(f"  • {name}：{cnt}")
	else:
		lines.append("  • 无")
	lines.append("")
	lines.append("累计各管理员：")
	if counts_total:
		for uid, display_name, username, cnt in counts_total:
			name = display_name or (f"@{username}" if username else str(uid))
			if username and display_name:
				name = f"{display_name} (@{username})"
			lines.append(f"  • {name}：{cnt}")
	else:
		lines.append("  • 无")

	await update.effective_chat.send_message("\n".join(lines))


async def cmd_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	"""Admin-only: Reply to a text or .txt document to batch upload in private chat."""
	user = update.effective_user
	chat = update.effective_chat
	if user is None or chat is None or not _is_admin(user.id) or chat.type != ChatType.PRIVATE:
		await update.effective_chat.send_message("无权限或上下文错误。请在私聊中使用该指令。")
		return

	if not update.effective_message or not update.effective_message.reply_to_message:
		await update.effective_chat.send_message("请回复一条文本或 .txt 文件消息后再发送 /上传。")
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
			await update.effective_chat.send_message("请回复 .txt 文件或纯文本消息。")
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
		await update.effective_chat.send_message("不支持的消息类型。请回复文本或 .txt 文件。")
		return

	if not codes:
		await update.effective_chat.send_message("未在被回复的消息中找到兑换码。")
		return

	inserted, duplicates = await storage.insert_codes(codes=codes, uploaded_by=user.id)
	remaining = await storage.count_unused()
	await update.effective_chat.send_message(
		f"上传成功：新增 {inserted} 条｜忽略重复 {duplicates} 条｜剩余未使用 {remaining} 条"
	)


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	"""Admin-only: Reset all codes to unused. Private chat only."""
	user = update.effective_user
	chat = update.effective_chat
	if user is None or chat is None or not _is_admin(user.id) or chat.type != ChatType.PRIVATE:
		await update.effective_chat.send_message("无权限或上下文错误。请在私聊中使用该指令。")
		return
	deleted = await storage.clear_all_codes()
	await update.effective_chat.send_message(
		f"已清空：共删除 {deleted} 条兑换码。"
	)


def main() -> None:
	# Create and set an event loop explicitly (Python 3.13 compatibility)
	loop = asyncio.new_event_loop()
	asyncio.set_event_loop(loop)
	# Initialize storage (async) before starting the bot
	loop.run_until_complete(storage.initialize())

	app = Application.builder().token(BOT_TOKEN).build()

	# Commands
	app.add_handler(CommandHandler("start", cmd_start))
	app.add_handler(CommandHandler("help", cmd_help))
	app.add_handler(CommandHandler("fa", cmd_distribute, filters=filters.ChatType.GROUPS))
	app.add_handler(CommandHandler("yuliang", cmd_remaining))
	app.add_handler(CommandHandler("yongliang", cmd_usage))
	app.add_handler(CommandHandler("shangchuan", cmd_upload))
	app.add_handler(CommandHandler("chongzhi", cmd_reset))

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


