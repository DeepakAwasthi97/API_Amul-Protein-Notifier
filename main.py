import asyncio
import json
import base64
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
from substore_list import substore_info
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)
from telegram.error import TelegramError
from datetime import datetime, timedelta
from collections import Counter
import time
import logging

# Local imports
import common
import config
if config.USE_DATABASE:
    from database import Database

logger = common.setup_logging()
logger.setLevel(logging.DEBUG)  # Ensure debug logging is enabled

# Initialize db as None; it will be set in run_polling if USE_DATABASE is True
db = None

# Conversation states
AWAITING_PINCODE, AWAITING_SUPPORT_MESSAGE, AWAITING_PRODUCT_SELECTION, AWAITING_ADMIN_REPLY = range(4)

async def update_users_file(users_data, context: ContextTypes.DEFAULT_TYPE):
    """Update the users.json file in the GitHub repository asynchronously."""
    max_retries = 3
    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                sha = await common.get_file_sha(config.USERS_FILE)
                if not sha:
                    logger.error("Failed to get SHA for %s on attempt %d", config.USERS_FILE, attempt + 1)
                    continue

                url = f"https://api.github.com/repos/{config.PRIVATE_REPO}/contents/{config.USERS_FILE}"
                headers = {
                    "Authorization": f"token {config.GH_PAT}",
                    "Accept": "application/vnd.github+json",
                }
                content = base64.b64encode(json.dumps(users_data, indent=2).encode()).decode()
                data = {
                    "message": "Update users.json with new user data",
                    "content": content,
                    "sha": sha,
                    "branch": config.GITHUB_BRANCH,
                }

                async with session.put(url, headers=headers, json=data) as response:
                    if response.status == 200:
                        logger.info("Successfully updated %s", config.USERS_FILE)
                        return True
                    logger.error(
                        "Failed to update %s on attempt %d: Status %d, Response: %s",
                        config.USERS_FILE,
                        attempt + 1,
                        response.status,
                        await response.text(),
                    )
            except Exception as e:
                logger.error("Error updating %s on attempt %d: %s", config.USERS_FILE, attempt + 1, str(e))
            if attempt < max_retries - 1:
                await asyncio.sleep(2)

        # Notify admin if all retries fail
        try:
            async with session.post(
                f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": config.ADMIN_CHAT_ID, "text": f"Failed to update {config.USERS_FILE} after {max_retries} attempts."}
            ) as response:
                if response.status != 200:
                    logger.error("Failed to notify admin: Status %d, Response: %s", response.status, await response.text())
        except Exception as e:
            logger.error("Error notifying admin of update failure: %s", str(e))
        return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /start command."""
    chat_id = update.effective_chat.id
    logger.info("Handling /start command for chat_id %s", common.mask(chat_id))
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    user = None
    users_data = None
    if config.USE_DATABASE:
        user = await db.get_user(chat_id)
    else:
        users_data = context.bot_data.get("users_data", common.read_users_file())
        context.bot_data["users_data"] = users_data
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)

    if user and user.get("pincode"):
        pincode = user.get("pincode")
        if user.get("active"):
            products = user.get("products", ["Any"])
            product_message = "All of the available Amul Protein products 🧀" if len(products) == 1 and products[0].lower() == "any" else "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in products)
            await update.message.reply_text(
                f"🎉 You have already enabled notifications for PINCODE {pincode} 📍.\n"
                f"You are currently tracking:\n{product_message}"
            )
        else:
            user["active"] = True
            if config.USE_DATABASE:
                await db.update_user(chat_id, user)
            else:
                user_in_data = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
                if user_in_data:
                    user_in_data.update(user)
                if not await update_users_file(users_data, context):
                    await update.message.reply_text("⚠️ Failed to re-enable notifications. Please try again.")
                    return
                context.bot_data["users_data"] = users_data
            message = await update.message.reply_text("⏳ Re-enabling notifications...")
            await asyncio.sleep(0.5)
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
            except TelegramError as e:
                logger.debug("Failed to delete transitional message for chat_id %s: %s", common.mask(chat_id), str(e))
            await update.message.reply_text(
                f"🎉 Welcome back! Notifications have been re-enabled for PINCODE {pincode} 📍.\n"
                "Use /stop to pause them again."
            )
    else:
        await update.message.reply_text(
            "👋 Welcome to the Amul Protein Items Notifier Bot! 🧀\n\n"
            "Use /setpincode PINCODE to set your pincode 📍 (Mandatory).\n"
            "Use /setproducts to select products 🧀 (Optional, defaults to any Amul protein product).\n"
            "Use /support to report issues or support the project 📞."
        )

async def _save_pincode(chat_id: int, pincode: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Helper function to save the pincode for a user, handling both DB and JSON file."""
    if config.USE_DATABASE:
        user = await db.get_user(chat_id)
        if user:
            user["pincode"] = pincode
            user["active"] = True
            await db.update_user(chat_id, user)
        else:
            new_user = {"chat_id": str(chat_id), "pincode": pincode, "products": ["Any"], "active": True}
            await db.add_user(chat_id, new_user)
        return True
    else:
        users_data = context.bot_data.get("users_data", common.read_users_file())
        users = users_data["users"]
        user = next((u for u in users if u["chat_id"] == str(chat_id)), None)
        if user:
            user["pincode"] = pincode
            user["active"] = True
        else:
            users.append({"chat_id": str(chat_id), "pincode": pincode, "products": ["Any"], "active": True})
        if await update_users_file(users_data, context):
            context.bot_data["users_data"] = users_data
            return True
        return False

async def set_pincode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the conversation to set a pincode or sets it directly if provided."""
    chat_id = update.effective_chat.id
    logger.info("Handling /setpincode command for chat_id %s", common.mask(chat_id))
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    if context.args:
        pincode = context.args[0]
        if not pincode.isdigit() or len(pincode) != 6:
            await update.message.reply_text("⚠️ PINCODE must be a 6-digit number.")
            return ConversationHandler.END

        message = await update.message.reply_text("⏳ Setting PINCODE...")
        await asyncio.sleep(0.5)
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
        except TelegramError as e:
            logger.debug("Failed to delete transitional message for chat_id %s: %s", common.mask(chat_id), str(e))

        if await _save_pincode(chat_id, pincode, context):
            await update.message.reply_text(f"✅ PINCODE set to {pincode} 📍. You will receive notifications for available products.")
        else:
            await update.message.reply_text("⚠️ Failed to update your PINCODE. Please try again.")
        return ConversationHandler.END
    else:
        await update.message.reply_text("📍 Please send me your 6-digit pincode.")
        return AWAITING_PINCODE

async def pincode_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the pincode received from the user during a conversation."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    pincode = update.message.text

    if not pincode.isdigit() or len(pincode) != 6:
        await update.message.reply_text("⚠️ That doesn't look like a valid 6-digit pincode. Please try again, or use /cancel to stop.")
        return AWAITING_PINCODE

    if await _save_pincode(chat_id, pincode, context):
        await update.message.reply_text(f"✅ Thank you! Your PINCODE has been set to {pincode} 📍.")
    else:
        await update.message.reply_text("⚠️ Failed to set your PINCODE. Please try again.")
    
    return ConversationHandler.END

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the /support command with a menu for Contact Me and Support Project."""
    chat_id = update.effective_chat.id
    logger.info("Handling /support command for chat_id %s", common.mask(chat_id))
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    keyboard = [
        [InlineKeyboardButton("Contact Me 📞", callback_data="support_contact")],
        [InlineKeyboardButton("Support Project 🌟", callback_data="support_project")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("📞 How can we assist you today?", reply_markup=reply_markup)
    return AWAITING_PRODUCT_SELECTION  # Enter state for callback handling

async def support_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the support menu callback actions."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    logger.debug("Support callback triggered for chat_id %s with action %s", common.mask(chat_id), query.data)
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    action = query.data
    if action == "support_contact":
        # Check rate limit (1 message every 5 minutes)
        last_support_time = context.user_data.get("last_support_time")
        if last_support_time and (datetime.now() - last_support_time) < timedelta(minutes=5):
            await query.edit_message_text("⏳ Please wait a few minutes before sending another support message.")
            return ConversationHandler.END

        await query.edit_message_text("📞 We're listening! Please send your feedback or issue. Use /cancel to stop.")
        return AWAITING_SUPPORT_MESSAGE
    elif action == "support_project":
        keyboard = [
            [InlineKeyboardButton("Give ⭐ on GitHub", callback_data="support_github")],
            [InlineKeyboardButton("Tip (chai/coffee) 😊", callback_data="support_tip")],
            [InlineKeyboardButton("⬅️ Go Back", callback_data="support_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("🌟 Support the project:\n- Give a star on GitHub.\n- Tip with chai or coffee!", reply_markup=reply_markup)
        return AWAITING_PRODUCT_SELECTION
    elif action == "support_github":
        keyboard = [
            [InlineKeyboardButton("⬅️ Go Back", callback_data="support_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("⭐ Thank you for supporting! Please visit https://github.com/DeepakAwasthi97/API_Amul-Protein-Notifier to give a star manually.", reply_markup=reply_markup)
        return AWAITING_PRODUCT_SELECTION
    elif action == "support_tip":
        keyboard = [
            [InlineKeyboardButton("⬅️ Go Back", callback_data="support_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("😊 Thank you for the tip! Please support via https://buymeachai.ankushminda.com/Deepak_Awasthi (ensure your page is set up).", reply_markup=reply_markup)
        return AWAITING_PRODUCT_SELECTION
    elif action == "support_back":
        keyboard = [
            [InlineKeyboardButton("Contact Me 📞", callback_data="support_contact")],
            [InlineKeyboardButton("Support Project 🌟", callback_data="support_project")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("📞 How can we assist you today?", reply_markup=reply_markup)
        return AWAITING_PRODUCT_SELECTION

async def support_message_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the support message received from the user during a conversation."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    message = update.message.text

    # Check rate limit (aligned to 5 minutes for consistency)
    last_support_time = context.user_data.get("last_support_time")
    if last_support_time and (datetime.now() - last_support_time) < timedelta(minutes=5):
        await update.message.reply_text("⏳ Please wait a few minutes before sending another support message.")
        return ConversationHandler.END

    if len(message) < 5:
        await update.message.reply_text("⚠️ Your message is too short. If this was a mistake, use /cancel to stop.")
        return AWAITING_SUPPORT_MESSAGE
    if len(message) > 500:
        await update.message.reply_text("⚠️ Your message is too long. Please keep it under 500 characters, or use /cancel to stop.")
        return AWAITING_SUPPORT_MESSAGE

    # Log the support message
    logger.info("User chat_id %s sent support message: %s", common.mask(chat_id), message)

    # Initialize support_requests in bot_data if not present
    if "support_requests" not in context.bot_data:
        context.bot_data["support_requests"] = {}

    # Get user data
    user = None
    if config.USE_DATABASE:
        user = await db.get_user(chat_id)
    else:
        users_data = context.bot_data.get("users_data", common.read_users_file())
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)

    # Prepare user info for admin
    user_info = f"Chat ID: {chat_id}\n"
    user_info += f"Pincode: {user.get('pincode', 'Not set')}\n"
    products = user.get("products", ["Any"]) if user else ["Any"]
    product_message = "All available Amul Protein products" if len(products) == 1 and products[0].lower() == "any" else "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in products)
    user_info += f"Tracked Products:\n{product_message}"

    # Escape the user's message and user_info
    def escape_markdown(text):
        special_chars = r'_*[]()~`>#+-=|{}.!-'
        return ''.join(f'\\{c}' if c in special_chars else c for c in text)

    escaped_user_info = escape_markdown(user_info)
    escaped_user_message = escape_markdown(message)

    # Construct the message with unescaped newlines in the template
    base_text = f"📞 *Support Request*\n\nUser Info:\n{escaped_user_info}\n\nMessage:\n{escaped_user_message}"

    # Store support request
    request_id = str(int(time.time() * 1000))  # Unique ID based on timestamp
    context.bot_data["support_requests"][request_id] = {
        "chat_id": str(chat_id),
        "message": message,
        "timestamp": datetime.now()
    }

    # Send to admin with Reply button
    try:
        await context.bot.send_message(
            chat_id=config.ADMIN_CHAT_ID,
            text=base_text,
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Reply", callback_data=f"reply_{request_id}")]])
        )
        context.user_data["last_support_time"] = datetime.now()
        await update.message.reply_text("✅ Thank you for your feedback! 📞 We've sent it to our team.")
    except Exception as e:
        logger.error(f"Failed to send support message for chat_id {chat_id}: {str(e)}")
        await update.message.reply_text("⚠️ Failed to send your message. Please try again later.")
    
    return ConversationHandler.END

async def reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /reply <chat_id> <message> command for the admin to send a message to any user."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    if str(chat_id) != config.ADMIN_CHAT_ID:
        logger.warning("Unauthorized reply attempt by chat_id %s", common.mask(chat_id))
        await update.message.reply_text("⚠️ You are not authorized to use this command.")
        return

    if len(context.args) < 2:
        await update.message.reply_text("⚠️ Usage: /reply <chat_id> <message>")
        return

    target_chat_id = context.args[0]
    message = " ".join(context.args[1:])

    # Validate chat_id
    try:
        target_chat_id = int(target_chat_id)
    except ValueError:
        await update.message.reply_text("⚠️ Invalid chat_id. It must be a number.")
        return

    # Validate message length
    if len(message) < 5:
        await update.message.reply_text("⚠️ Your message is too short. Please provide at least 5 characters.")
        return
    if len(message) > 4096:
        await update.message.reply_text("⚠️ Your message is too long. Please keep it under 4096 characters.")
        return

    # Animation effect
    transitional_message = await update.message.reply_text("⏳ Sending reply...")
    await asyncio.sleep(0.5)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=transitional_message.message_id)
    except TelegramError as e:
        logger.debug("Failed to delete transitional message for chat_id %s: %s", common.mask(chat_id), str(e))

    # Send reply to user with MarkdownV2 formatting
    try:
        special_chars = r'_*[]()~`>#+-=|{}.!'
        escaped_message = ''.join(f'\\{c}' if c in special_chars else c for c in message)
        await context.bot.send_message(
            chat_id=target_chat_id,
            text=f"📩 *Reply from Support Team*:\n\n{escaped_message}",
            parse_mode="MarkdownV2"
        )
        await update.message.reply_text(f"✅ Reply sent to user {target_chat_id}.")
    except TelegramError as e:
        logger.error("Failed to send reply to chat_id %s: %s", common.mask(target_chat_id), str(e))
        await update.message.reply_text(f"⚠️ Failed to send reply to {target_chat_id}. The user may have blocked the bot or the chat_id is invalid.")

async def reply_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the reply action for a support request."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    if str(chat_id) != config.ADMIN_CHAT_ID:
        logger.warning(f"Unauthorized reply attempt by chat_id {chat_id}")
        return ConversationHandler.END

    request_id = query.data.replace("reply_", "")
    support_request = context.bot_data["support_requests"].get(request_id)

    if not support_request:
        # Send a new message instead of editing the original
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ Support request not found. Please start over."
        )
        return ConversationHandler.END

    user_chat_id = support_request["chat_id"]
    # Always start a new reply session
    if "reply_sessions" not in context.user_data:
        context.user_data["reply_sessions"] = {}
    context.user_data["reply_sessions"][request_id] = {
        "chat_id": user_chat_id,
        "timestamp": datetime.now()
    }
    
    # Send a new message for the reply prompt with the specific request ID
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"📝 Enter your reply to send to chat_id {user_chat_id} (Request ID: {request_id}). Use /cancel to stop.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Cancel", callback_data=f"cancel_reply_{request_id}")]])
    )
    
    return AWAITING_ADMIN_REPLY

async def admin_reply_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the admin's reply message and sends it to the user."""
    chat_id = update.effective_chat.id
    message = update.message.text

    if message == "/cancel":
        await update.message.reply_text("❌ Reply canceled.")
        # Clear all reply sessions
        context.user_data.pop("reply_sessions", None)
        return ConversationHandler.END

    if len(message) > 4096:
        await update.message.reply_text("⚠️ Reply too long. Please keep it under 4096 characters.")
        return AWAITING_ADMIN_REPLY

    # Retrieve the active reply session
    reply_sessions = context.user_data.get("reply_sessions", {})
    if not reply_sessions:
        await update.message.reply_text("⚠️ No active reply session. Please use the 'Reply' button to start.")
        return ConversationHandler.END

    # Use the most recent session or allow admin to specify request_id
    request_id = None
    for req_id, session in reply_sessions.items():
        if datetime.now() - session["timestamp"] < timedelta(minutes=30):  # 30-minute session timeout
            request_id = req_id
            break

    if not request_id:
        await update.message.reply_text("⚠️ No valid reply session found. Please use the 'Reply' button to start.")
        context.user_data.pop("reply_sessions", None)
        return ConversationHandler.END

    support_request = context.bot_data["support_requests"].get(request_id)

    if not support_request:
        await update.message.reply_text("⚠️ Support request not found. Please start over.")
        context.user_data.pop("reply_sessions", None)
        return ConversationHandler.END

    user_chat_id = support_request["chat_id"]

    try:
        await context.bot.send_message(
            chat_id=user_chat_id,
            text=f"📩 *Admin Reply*:\n\n{message}",
            parse_mode="MarkdownV2"
        )
        await update.message.reply_text(f"✅ Reply sent to chat_id {user_chat_id} (Request ID: {request_id}).")
        logger.info(f"Admin {chat_id} sent reply to chat_id {user_chat_id} for request {request_id}")
        del context.bot_data["support_requests"][request_id]
        context.user_data["reply_sessions"].pop(request_id, None)
        if not context.user_data["reply_sessions"]:
            context.user_data.pop("reply_sessions", None)
    except Exception as e:
        logger.error(f"Failed to send reply to chat_id {user_chat_id} for request {request_id}: {str(e)}")
        await update.message.reply_text("⚠️ Failed to send reply. Please try again.")

    return ConversationHandler.END

async def cancel_reply_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the cancellation of the reply action."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    request_id = query.data.replace("cancel_reply_", "")
    await query.edit_message_text(f"❌ Reply action canceled for Request ID: {request_id}.")
    # Clear the specific reply session
    if "reply_sessions" in context.user_data and request_id in context.user_data["reply_sessions"]:
        context.user_data["reply_sessions"].pop(request_id, None)
    if not context.user_data.get("reply_sessions"):
        context.user_data.pop("reply_sessions", None)
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation with an animation effect."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    
    message = await update.message.reply_text("❌ Cancelling...")
    await asyncio.sleep(0.5)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
    except TelegramError as e:
        logger.debug("Failed to delete transitional message for chat_id %s: %s", common.mask(chat_id), str(e))

    # Clear product selection and reply state
    for key in ["selected_products", "product_menu_view", "product_menu_category", "cached_user", "reply_sessions"]:
        context.user_data.pop(key, None)
    await update.message.reply_text("❌ Action cancelled.")
    return ConversationHandler.END

async def cleanup_support_requests(context: ContextTypes.DEFAULT_TYPE):
    """Periodically clean up support requests older than 24 hours."""
    if "support_requests" not in context.bot_data:
        return
    cutoff = datetime.now() - timedelta(hours=24)
    support_requests = context.bot_data["support_requests"]
    expired = [req_id for req_id, req in support_requests.items() if req["timestamp"] < cutoff]
    for req_id in expired:
        support_requests.pop(req_id, None)
    logger.debug("Cleaned up %d expired support requests", len(expired))

async def set_products(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the product selection conversation, clearing previous state."""
    chat_id = update.effective_chat.id
    logger.info("Starting product selection for chat_id %s", common.mask(chat_id))
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    start_time = time.time()

    # Clear previous product selection state
    for key in ["selected_products", "product_menu_view", "product_menu_category", "cached_user"]:
        context.user_data.pop(key, None)

    if config.USE_DATABASE:
        user = await db.get_user(chat_id)
    else:
        users_data = context.bot_data.get("users_data", common.read_users_file())
        context.bot_data["users_data"] = users_data
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)

    context.user_data["selected_products"] = set()
    context.user_data["product_menu_view"] = "main"
    context.user_data["cached_user"] = user

    keyboard = [
        [InlineKeyboardButton("Browse by Category 🧀", callback_data="products_nav_cat_list")],
        [InlineKeyboardButton("List All Products 📋", callback_data="products_nav_all")],
        [InlineKeyboardButton("Track Any Available Product ❗", callback_data="products_confirm_Any")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "🧀 Please select the products you want to monitor.", reply_markup=reply_markup
    )
    logger.info("set_products took %.2f seconds for chat_id %s", time.time() - start_time, common.mask(chat_id))
    return AWAITING_PRODUCT_SELECTION

async def set_products_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles all interactions for the product selection menu with animation effects."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    start_time = time.time()

    # Ensure selected_products is initialized
    if "selected_products" not in context.user_data:
        context.user_data["selected_products"] = set()
    selected_products = context.user_data["selected_products"]
    logger.debug("Selected products for chat_id %s: %s", common.mask(chat_id), selected_products)

    action = query.data
    action_for_rendering = action

    try:
        if action == "products_nav_main" and selected_products:
            display_products = [common.PRODUCT_NAME_MAP.get(p, p) for p in selected_products if p in common.PRODUCTS]
            product_list_text = "\n".join(f"- {p}" for p in display_products if p)
            if not product_list_text:
                product_list_text = "No valid products selected."
                logger.warning("No valid products in display_products for chat_id %s: %s", common.mask(chat_id), display_products)
            confirmation_text = (
                "🧀 Please confirm your selection of products for notifications:\n\n" +
                f"{product_list_text}"
            )
            confirmation_keyboard = [
                [InlineKeyboardButton("✅ Confirm Selection", callback_data="products_confirm")],
                [InlineKeyboardButton("❌ Clear Selection & Back to Main Menu", callback_data="products_clear_and_back_to_main")],
            ]
            reply_markup = InlineKeyboardMarkup(confirmation_keyboard)
            await query.edit_message_text(text=confirmation_text, reply_markup=reply_markup)
            logger.info("Confirmation menu rendered in %.2f seconds for chat_id %s", time.time() - start_time, common.mask(chat_id))
            return AWAITING_PRODUCT_SELECTION

        if action.startswith("products_toggle_"):
            try:
                product_index = int(action.replace("products_toggle_", ""))
                if product_index < 0 or product_index >= len(common.PRODUCTS):
                    logger.error("Invalid product_index %d for chat_id %s", product_index, common.mask(chat_id))
                    try:
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Processing error..."
                        )
                        await asyncio.sleep(0.5)
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Invalid product selection. Please try again or use /setproducts to restart."
                        )
                    except TelegramError as e:
                        logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="⚠️ Invalid product selection. Please try again or use /setproducts to restart."
                        )
                    return AWAITING_PRODUCT_SELECTION
                product_name = common.PRODUCTS[product_index]
                if product_name in selected_products:
                    selected_products.remove(product_name)
                else:
                    selected_products.add(product_name)
                logger.debug("Toggled product %s for chat_id %s, new selected_products: %s", product_name, common.mask(chat_id), selected_products)
            except ValueError:
                logger.error("Invalid product_toggle action for chat_id %s: %s", common.mask(chat_id), action)
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        text="⚠️ Processing error..."
                    )
                    await asyncio.sleep(0.5)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        text="⚠️ Invalid product selection. Please try again or use /setproducts to restart."
                    )
                except TelegramError as e:
                    logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="⚠️ Invalid product selection. Please try again or use /setproducts to restart."
                    )
                return AWAITING_PRODUCT_SELECTION
            action_for_rendering = f"products_view_cat_{context.user_data.get('product_menu_category', '')}" if context.user_data.get("product_menu_view") == "category" else "products_nav_all"

        elif action == "products_clear":
            current_category = context.user_data.get("product_menu_category")
            if not selected_products:
                text = "🧀 No products selected to clear."
                keyboard = []
                if current_category:
                    text += f"\n\nProducts in {current_category}:"
                    for product_name in common.CATEGORIZED_PRODUCTS[current_category]:
                        product_index = common.PRODUCTS.index(product_name)
                        selected_marker = "✅ " if product_name in selected_products else ""
                        keyboard.append([InlineKeyboardButton(f"{selected_marker}{common.PRODUCT_NAME_MAP.get(product_name, product_name)}", callback_data=f"products_toggle_{product_index}")])
                    keyboard.append([
                        InlineKeyboardButton("✅ Confirm Selection", callback_data="products_confirm"),
                        InlineKeyboardButton("❌ Clear Selection", callback_data="products_clear"),
                    ])
                    keyboard.append([InlineKeyboardButton("⬅️ Back to Categories", callback_data="products_nav_cat_list")])
                else:
                    text += "\n\nSelect products to monitor:"
                    for i, product_name in enumerate(common.PRODUCTS):
                        if product_name == "Any": continue
                        selected_marker = "✅ " if product_name in selected_products else ""
                        keyboard.append([InlineKeyboardButton(f"{selected_marker}{common.PRODUCT_NAME_MAP.get(product_name, product_name)}", callback_data=f"products_toggle_{i}")])
                    keyboard.append([
                        InlineKeyboardButton("✅ Confirm Selection", callback_data="products_confirm"),
                        InlineKeyboardButton("❌ Clear Selection", callback_data="products_clear"),
                    ])
                    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="products_nav_main")])
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(text=text, reply_markup=reply_markup)
                logger.info("Clear selection (no products) took %.2f seconds for chat_id %s", time.time() - start_time, common.mask(chat_id))
                return AWAITING_PRODUCT_SELECTION
            if current_category:
                products_to_clear = set(common.CATEGORIZED_PRODUCTS[current_category])
                selected_products -= products_to_clear
            else:
                selected_products.clear()
            logger.debug("Cleared products for chat_id %s, new selected_products: %s", common.mask(chat_id), selected_products)
            action_for_rendering = f"products_view_cat_{current_category}" if current_category else "products_nav_all"

        elif action == "products_confirm_Any":
            final_selection = ["Any"]
            user = context.user_data.get("cached_user", {})
            user["products"] = final_selection
            user["active"] = True
            logger.debug("Confirm Any for chat_id %s, final_selection: %s", common.mask(chat_id), final_selection)
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=query.message.message_id,
                    text="✅ Saving your selection..."
                )
                await asyncio.sleep(0.5)
            except TelegramError as e:
                logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
            if config.USE_DATABASE:
                try:
                    await db.update_user(chat_id, user)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        text="✅ Your selection has been saved. You will be notified if **any** Amul Protein product is available ❗."
                    )
                except Exception as e:
                    logger.error("Database error for chat_id %s: %s", common.mask(chat_id), str(e))
                    try:
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Processing error..."
                        )
                        await asyncio.sleep(0.5)
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Failed to save your selection. Please try again later."
                        )
                    except TelegramError as e:
                        logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="⚠️ Failed to save your selection. Please try again later."
                        )
            else:
                users_data = context.bot_data.get("users_data", common.read_users_file())
                user_in_data = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
                if user_in_data:
                    user_in_data.update(user)
                else:
                    users_data["users"].append(user)
                if await update_users_file(users_data, context):
                    context.bot_data["users_data"] = users_data
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        text="✅ Your selection has been saved. You will be notified if **any** Amul Protein product is available ❗."
                    )
                else:
                    try:
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Processing error..."
                        )
                        await asyncio.sleep(0.5)
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Failed to save your selection. Please try again later."
                        )
                    except TelegramError as e:
                        logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="⚠️ Failed to save your selection. Please try again later."
                        )
            for key in [key for key in context.user_data if key.startswith("product_menu_")]:
                del context.user_data[key]
            context.user_data["selected_products"] = set()
            logger.info("Confirm Any took %.2f seconds for chat_id %s", time.time() - start_time, common.mask(chat_id))
            return ConversationHandler.END  # End conversation after confirmation

        elif action == "products_confirm":
            if not selected_products:
                user = context.user_data.get("cached_user", {})
                current_tracked_products = user.get("products", ["Any"])
                product_message = "All of the available Amul Protein products 🧀" if len(current_tracked_products) == 1 and current_tracked_products[0].lower() == "any" else "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in current_tracked_products)
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        text="⚠️ Processing..."
                    )
                    await asyncio.sleep(0.5)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        text=f"⚠️ No products were selected. You are currently tracking:\n{product_message}"
                    )
                except TelegramError as e:
                    logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"⚠️ No products were selected. You are currently tracking:\n{product_message}"
                    )
                for key in [key for key in context.user_data if key.startswith("product_menu_")]:
                    del context.user_data[key]
                context.user_data["selected_products"] = set()
                logger.info("Confirm (no selection) took %.2f seconds for chat_id %s", time.time() - start_time, common.mask(chat_id))
                return ConversationHandler.END  # End conversation after no selection

            final_selection = ["Any"] if "Any" in selected_products else list(selected_products)
            user = context.user_data.get("cached_user", {})
            user["products"] = final_selection
            user["active"] = True
            product_message = "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in final_selection if common.PRODUCT_NAME_MAP.get(p, p))
            if not product_message:
                product_message = "No valid product names available."
                logger.warning("Empty product message for chat_id %s, final_selection: %s", common.mask(chat_id), final_selection)
            
            logger.debug("Confirm selection for chat_id %s, final_selection: %s", common.mask(chat_id), final_selection)
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=query.message.message_id,
                    text="✅ Saving your selection..."
                )
                await asyncio.sleep(0.5)
            except TelegramError as e:
                logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
            if config.USE_DATABASE:
                try:
                    await db.update_user(chat_id, user)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        text=f"✅ Your selections have been saved. You will be notified for:\n{product_message}"
                    )
                except Exception as e:
                    logger.error("Database error for chat_id %s: %s", common.mask(chat_id), str(e))
                    try:
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Processing error..."
                        )
                        await asyncio.sleep(0.5)
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Failed to save your selections. Please try again later."
                        )
                    except TelegramError as e:
                        logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="⚠️ Failed to save your selections. Please try again later."
                        )
            else:
                users_data = context.bot_data.get("users_data", common.read_users_file())
                user_in_data = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
                if user_in_data:
                    user_in_data.update(user)
                else:
                    users_data["users"].append(user)
                if await update_users_file(users_data, context):
                    context.bot_data["users_data"] = users_data
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        text=f"✅ Your selections have been saved. You will be notified for:\n{product_message}"
                    )
                else:
                    try:
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Processing error..."
                        )
                        await asyncio.sleep(0.5)
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=query.message.message_id,
                            text="⚠️ Failed to save your selections. Please try again later."
                        )
                    except TelegramError as e:
                        logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text="⚠️ Failed to save your selections. Please try again later."
                        )
            for key in [key for key in context.user_data if key.startswith("product_menu_")]:
                del context.user_data[key]
            context.user_data["selected_products"] = set()
            logger.info("Confirm selection took %.2f seconds for chat_id %s", time.time() - start_time, common.mask(chat_id))
            return ConversationHandler.END  # End conversation after confirmation

        elif action == "products_clear_and_back_to_main":
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=query.message.message_id,
                    text="❌ Clearing selection..."
                )
                await asyncio.sleep(0.5)
            except TelegramError as e:
                logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
            selected_products.clear()
            for key in [key for key in context.user_data if key.startswith("product_menu_")]:
                del context.user_data[key]
            context.user_data["product_menu_view"] = "main"
            action_for_rendering = "products_nav_main"

        # Menu rendering
        keyboard = []
        text = ""
        if action_for_rendering == "products_nav_main":
            context.user_data["product_menu_view"] = "main"
            text = "🧀 Please select the products you want to monitor."
            keyboard.extend([
                [InlineKeyboardButton("Browse by Category 🧀", callback_data="products_nav_cat_list")],
                [InlineKeyboardButton("List All Products 📋", callback_data="products_nav_all")],
                [InlineKeyboardButton("Track Any Available Product ❗", callback_data="products_confirm_Any")],
            ])
        elif action_for_rendering == "products_nav_cat_list":
            context.user_data["product_menu_view"] = "cat_list"
            text = "🧀 Select a category to view products:"
            for category in common.CATEGORIES:
                keyboard.append([InlineKeyboardButton(category, callback_data=f"products_view_cat_{category}")])
        elif action_for_rendering.startswith("products_view_cat_"):
            category = action_for_rendering.replace("products_view_cat_", "")
            context.user_data["product_menu_view"] = "category"
            context.user_data["product_menu_category"] = category
            text = f"🧀 Products in {category}:"
            for product_name in common.CATEGORIZED_PRODUCTS[category]:
                product_index = common.PRODUCTS.index(product_name)
                selected_marker = "✅ " if product_name in selected_products else ""
                keyboard.append([InlineKeyboardButton(f"{selected_marker}{common.PRODUCT_NAME_MAP.get(product_name, product_name)}", callback_data=f"products_toggle_{product_index}")])
        elif action_for_rendering == "products_nav_all":
            context.user_data["product_menu_view"] = "all"
            text = "🧀 Select products to monitor:"
            for i, product_name in enumerate(common.PRODUCTS):
                if product_name == "Any": continue
                selected_marker = "✅ " if product_name in selected_products else ""
                keyboard.append([InlineKeyboardButton(f"{selected_marker}{common.PRODUCT_NAME_MAP.get(product_name, product_name)}", callback_data=f"products_toggle_{i}")])

        if action_for_rendering not in ["products_nav_main", "products_nav_cat_list"]:
            keyboard.append([
                InlineKeyboardButton("✅ Confirm Selection", callback_data="products_confirm"),
                InlineKeyboardButton("❌ Clear Selection", callback_data="products_clear"),
            ])

        if context.user_data.get("product_menu_view") == "cat_list":
            keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="products_nav_main")])
        elif context.user_data.get("product_menu_view") == "category":
            keyboard.append([InlineKeyboardButton("⬅️ Back to Categories", callback_data="products_nav_cat_list")])
        elif context.user_data.get("product_menu_view") == "all":
            keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="products_nav_main")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text=text, reply_markup=reply_markup)
        logger.info("Menu rendering took %.2f seconds for chat_id %s", time.time() - start_time, common.mask(chat_id))
        return AWAITING_PRODUCT_SELECTION

    except Exception as e:
        logger.error("Error in set_products_callback for chat_id %s: %s", common.mask(chat_id), str(e))
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=query.message.message_id,
                text="⚠️ Processing error..."
            )
            await asyncio.sleep(0.5)
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=query.message.message_id,
                text="⚠️ An error occurred. Please try again or use /setproducts to restart."
            )
        except TelegramError as e:
            logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
            await context.bot.send_message(
                chat_id=chat_id,
                text="⚠️ An error occurred. Please try again or use /setproducts to restart."
            )
        for key in [key for key in context.user_data if key.startswith("product_menu_")]:
            del context.user_data[key]
        context.user_data["selected_products"] = set()
        logger.info("Error handling took %.2f seconds for chat_id %s", time.time() - start_time, common.mask(chat_id))
        return ConversationHandler.END  # End conversation on error

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /stop command with animation effect."""
    chat_id = update.effective_chat.id
    logger.info("Handling /stop command for chat_id %s", common.mask(chat_id))
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    message = await update.message.reply_text("⏳ Stopping notifications...")
    await asyncio.sleep(0.5)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
    except TelegramError as e:
        logger.debug("Failed to delete transitional message for chat_id %s: %s", common.mask(chat_id), str(e))

    if config.USE_DATABASE:
        user = await db.get_user(chat_id)
        if not user or not user.get("active", False):
            await update.message.reply_text("⚠️ You are not subscribed to notifications.")
            return
        user["active"] = False
        await db.update_user(chat_id, user)
    else:
        users_data = context.bot_data.get("users_data", common.read_users_file())
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
        if not user or not user.get("active", False):
            await update.message.reply_text("⚠️ You are not subscribed to notifications.")
            return
        user["active"] = False
        if not await update_users_file(users_data, context):
            await update.message.reply_text("⚠️ Failed to stop notifications. Please try again.")
            return
        context.bot_data["users_data"] = users_data

    keyboard = [[InlineKeyboardButton("🔄 Re-enable Notifications", callback_data="reactivate")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("⏸️ Notifications stopped.", reply_markup=reply_markup)

async def reactivate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback handler for the 'Re-enable Notifications' button with animation effect."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=query.message.message_id,
            text="⏳ Re-enabling notifications..."
        )
        await asyncio.sleep(0.5)
    except TelegramError as e:
        logger.debug("Failed to edit transitional message for chat_id %s: %s", common.mask(chat_id), str(e))

    user = None
    users_data = None
    if config.USE_DATABASE:
        user = await db.get_user(chat_id)
    else:
        users_data = context.bot_data.get("users_data", common.read_users_file())
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)

    if user and user.get("pincode"):
        if not user.get("active"):
            user["active"] = True
            if config.USE_DATABASE:
                await db.update_user(chat_id, user)
            else:
                user_in_data = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
                if user_in_data:
                    user_in_data.update(user)
                if not await update_users_file(users_data, context):
                    await query.edit_message_text("⚠️ Failed to re-enable notifications. Please try again.")
                    return
                context.bot_data["users_data"] = users_data
        
        await query.edit_message_text(f"🎉 Welcome back! Notifications have been re-enabled for PINCODE {user['pincode']} 📍.\nUse /stop to pause them again.")
    else:
        await query.edit_message_text("⚠️ Could not find your registration. Please use /start to set up notifications.")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
       chat_id = update.effective_chat.id
       await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

       if str(chat_id) != config.ADMIN_CHAT_ID:
           await update.message.reply_text("⚠️ You are not authorized to use this command.")
           return

       # Parse the command and message
       args = update.message.text.split(maxsplit=1)
       if len(args) < 2:
           await update.message.reply_text("⚠️ Usage: /broadcast <ALL|active|inactive> <message>")
           return

       target_group = args[0].lower().replace("/broadcast", "").strip()
       message_to_broadcast = args[1].strip()

       if len(message_to_broadcast) > 4096:
           await update.message.reply_text("⚠️ Message too long. Please keep it under 4096 characters.")
           return

       # Determine target users based on parameter
       if config.USE_DATABASE:
           all_users = await db.get_all_users()
           if not all_users or not isinstance(all_users, list):
               logger.error("get_all_users returned invalid data: %s", all_users)
               await update.message.reply_text("⚠️ Error fetching users from database.")
               return
           for user in all_users:
               if not all(k in user for k in ['chat_id', 'active', 'pincode', 'products']):
                   logger.warning("User data missing expected keys: %s", user)
       else:
           users_data = context.bot_data.get("users_data", common.read_users_file())
           all_users = users_data.get("users", [])
           if not all_users or not isinstance(all_users, list):
               logger.error("users.json data invalid: %s", all_users)
               await update.message.reply_text("⚠️ Error fetching users from JSON.")
               return

       target_users = []
       if target_group == "all":
           target_users = all_users
       elif target_group == "active":
           target_users = [user for user in all_users if user.get('active', False)]
       elif target_group == "inactive":
           target_users = [user for user in all_users if not user.get('active', False)]
       else:
           await update.message.reply_text("⚠️ Invalid target group. Use ALL, active, or inactive.")
           return

       if not target_users:
           await update.message.reply_text("⚠️ No users found for the selected target group.")
           return

       context.user_data['broadcast_message'] = message_to_broadcast
       context.user_data['broadcast_target'] = target_group

       # Escape the broadcast message
       def escape_markdown(text):
           special_chars = r'_*[]()~`>#+-=|{}.!-'
           return ''.join(f'\\{c}' if c in special_chars else c for c in text)

       base_text = f"📢 You are about to send the following message to {target_group} users:\n\n---\n{message_to_broadcast}\n---\n\nPlease confirm."
       escaped_full_text = escape_markdown(base_text)

       keyboard = [
           [InlineKeyboardButton("✅ Accept", callback_data='broadcast_accept')],
           [InlineKeyboardButton("❌ Reject", callback_data='broadcast_reject')],
       ]
       reply_markup = InlineKeyboardMarkup(keyboard)
       await update.message.reply_text(
           escaped_full_text,
           parse_mode="MarkdownV2",
           reply_markup=reply_markup
       )

async def send_broadcast_job(context: ContextTypes.DEFAULT_TYPE):
       job_data = context.job.data
       chat_id = job_data["chat_id"]
       message = job_data["message"]

       try:
           await context.bot.send_message(
               chat_id=chat_id,
               text=message,
               parse_mode="MarkdownV2"
           )
           logger.info("Broadcast sent to chat_id %s", common.mask(chat_id))
       except Exception as e:
           logger.error("Failed to send broadcast to chat_id %s: %s", common.mask(chat_id), str(e))

async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
       query = update.callback_query
       await query.answer()
       chat_id = query.from_user.id
       await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
       
       if str(chat_id) != config.ADMIN_CHAT_ID:
           return

       try:
           await context.bot.edit_message_text(
               chat_id=chat_id,
               message_id=query.message.message_id,
               text="⏳ Processing broadcast..."
           )
           await asyncio.sleep(0.5)
       except TelegramError as e:
           logger.debug("Failed to edit transitional message for chat_id %s: %s", common.mask(chat_id), str(e))

       if query.data == 'broadcast_accept':
           message = context.user_data.get('broadcast_message')
           target_group = context.user_data.get('broadcast_target')
           if not message:
               await query.edit_message_text("⚠️ Error: Broadcast message not found. Please try again.")
               return

           # Escape the broadcast message
           special_chars = r'_*[]()~`>#+-=|{}.!-'
           escaped_message = ''.join(f'\\{c}' if c in special_chars else c for c in message)

           if config.USE_DATABASE:
               all_users = await db.get_all_users()
               if not all_users or not isinstance(all_users, list):
                   logger.error("get_all_users returned invalid data: %s", all_users)
                   await query.edit_message_text("⚠️ Error fetching users from database.")
                   return
               for user in all_users:
                   if not all(k in user for k in ['chat_id', 'active', 'pincode', 'products']):
                       logger.warning("User data missing expected keys: %s", user)
           else:
               users_data = context.bot_data.get("users_data", common.read_users_file())
               all_users = users_data.get("users", [])
               if not all_users or not isinstance(all_users, list):
                   logger.error("users.json data invalid: %s", all_users)
                   await query.edit_message_text("⚠️ Error fetching users from JSON.")
                   return

           target_users = []
           if target_group == "all":
               target_users = all_users
           elif target_group == "active":
               target_users = [user for user in all_users if user.get('active', False)]
           elif target_group == "inactive":
               target_users = [user for user in all_users if not user.get('active', False)]

           sent_count = 0
           delay = 0.1  # Delay between sends to respect Telegram API limits

           for user in target_users:
               # Ensure chat_id is an integer, handling potential string or integer input
               chat_id_value = int(user['chat_id']) if isinstance(user['chat_id'], (str, int)) else user['chat_id']
               context.job_queue.run_once(
                   send_broadcast_job,
                   when=delay * sent_count,
                   data={"chat_id": chat_id_value, "message": escaped_message}
               )
               sent_count += 1

           await query.edit_message_text(f"✅ Broadcast queued for {sent_count} {target_group} users 📢. It will be sent in the background.")
           logger.info("Admin %s queued broadcast to %d %s users.", common.mask(chat_id), sent_count, target_group)
           context.user_data.pop('broadcast_message', None)
           context.user_data.pop('broadcast_target', None)

       elif query.data == 'broadcast_reject':
           await query.edit_message_text("❌ Broadcast canceled.")
           logger.info("Admin %s canceled broadcast.", common.mask(chat_id))
           context.user_data.pop("broadcast_message", None)
           context.user_data.pop("broadcast_target", None)

async def bot_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if str(chat_id) != config.ADMIN_CHAT_ID:
        await update.message.reply_text("⚠️ You are not authorized to use this command.")
        return
    
    # Fetch user data
    if config.USE_DATABASE:
        all_users = await db.get_all_users()
    else:
        users_data = context.bot_data.get("users_data", common.read_users_file())
        all_users = users_data.get("users", [])
    
    # Total Users and Active Users
    total_users = len(all_users)
    total_active_users = sum(1 for user in all_users if user.get('active', False))
    
    # Total Distinct Pincodes
    distinct_pincodes = len(set(user.get('pincode') for user in all_users if user.get('pincode')))
    
    # Top 3 States (using substore_list.py)
    pincode_to_state = {}
    for state_data in substore_info:
        state = state_data["name"]
        if state_data["pincodes"]:
            pincodes = state_data["pincodes"].split(",")
            for pincode in pincodes:
                pincode_to_state[pincode.strip()] = state
    
    user_states = [pincode_to_state.get(user.get("pincode", "").strip(), "Unknown") 
                   for user in all_users]
    known_user_states = [state for state in user_states if state != "Unknown"]
    state_counts = Counter(known_user_states)
    top_states = state_counts.most_common(3)
    
    # Top 3 Tracked Products (excluding "Any")
    product_counts = Counter()
    for user in all_users:
        products = user.get('products', [])
        if products != ["Any"]:
            product_counts.update(products)
    top_products = product_counts.most_common(3)
    
    # Users Not Tracking Specific Products
    users_not_tracking_specific = [
        user['chat_id'] for user in all_users if user.get('products') == ["Any"] or not user.get('products')
    ]
    
    # Additional Stats
    total_support_requests = len(context.bot_data.get("support_requests", {}))
    pincode_distribution = Counter(user.get('pincode') for user in all_users if user.get('pincode'))
    
    # Format the stats message with bold stat names
    stats_message = (
        f"📊 *Bot Statistics* 📊\n\n"
        f"👥 *Total Users*: {total_users}\n"
        f"✅ *Total Active Users*: {total_active_users}\n"
        f"🏙️ *Total Unqiue Pincodes*: {distinct_pincodes}\n\n"
        f"🏳️ *Top 3 States*:\n"
    )
    
    for state, count in top_states:
        stats_message += f"  - {state}: {count} users\n"
    
    stats_message += f"\n🔝 *Top 3 Tracked Products*:\n"
    if top_products:
        # stats_message += f"  Most Tracked products:\n"
        for product, count in top_products:
            stats_message += f"  - {count} users are tracking {common.PRODUCT_NAME_MAP.get(product, product)} \n"
    else:
        stats_message += "  No specific products are currently tracked.\n"
    
    stats_message += (
        f"\n📦 *Users Not Tracking Specific Products*: {len(users_not_tracking_specific)}\n"
        # f"📞 *Total Support Requests*: {total_support_requests}\n"
        f"\n🏙️ *Pincode Distribution (Top 5)*:\n"
    )
    
    for pincode, count in pincode_distribution.most_common(5):
        stats_message += f"  - {pincode}: {count} users\n"
    
    await update.message.reply_text(stats_message, parse_mode='Markdown')

async def run_polling(app: Application):
    """Starts the bot in polling mode."""
    global db
    if config.USE_DATABASE:
        db = Database(config.DATABASE_FILE)
        await db._init_db()
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling(timeout=5)
    logger.info("Polling started")
    # Schedule periodic cleanup of support requests
    app.job_queue.run_repeating(cleanup_support_requests, interval=3600)  # Run every hour
    try:
        await asyncio.Event().wait()
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Polling stopped")
    finally:
        logger.info("Shutting down bot...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        if config.USE_DATABASE and db:
            await db.close()
        logger.info("Bot shutdown complete")

def main():
    """Main entry point for the bot."""
    logger.info("Starting main function")
    if common.is_already_running("main.py"):
        logger.error("Another instance of the bot is already running. Exiting...")
        raise SystemExit(1)

    app = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()

    # Combined conversation handler for pincode, support, product selection, and admin reply
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("setpincode", set_pincode),
            CommandHandler("support", support),
            CommandHandler("setproducts", set_products),
            CallbackQueryHandler(reply_callback, pattern='^reply_'),
        ],
        states={
            AWAITING_PINCODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, pincode_received)],
            AWAITING_SUPPORT_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, support_message_received)],
            AWAITING_PRODUCT_SELECTION: [
                CallbackQueryHandler(support_callback, pattern='^support_'),
                CallbackQueryHandler(set_products_callback, pattern='^products_')
            ],
            AWAITING_ADMIN_REPLY: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_reply_received)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_conversation),
            CommandHandler("support", support),
            CommandHandler("setproducts", set_products)  # Allow /support to restart the conversation
        ],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reply", reply))  # Add direct /reply command
    app.add_handler(CallbackQueryHandler(cancel_reply_callback, pattern='^cancel_reply_'))
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CallbackQueryHandler(reactivate_callback, pattern='^reactivate$'))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern='^broadcast_'))
    app.add_handler(CommandHandler("broadcast", broadcast))  # Add broadcast command handler
    app.add_handler(CommandHandler("bot_stats", bot_stats))

    asyncio.run(run_polling(app))

if __name__ == "__main__":
    main()