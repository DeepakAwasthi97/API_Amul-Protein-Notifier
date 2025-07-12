import asyncio
import json
import base64
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
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
import time

# Local imports
import common
import config
if config.USE_DATABASE:
    from database import Database

logger = common.setup_logging()

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
            # Animation effect
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
            "Use /support to report issues or share feedback 📞."
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

        # Animation effect
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
    """Handles the /support command for reporting issues or sending feedback."""
    chat_id = update.effective_chat.id
    logger.info("Handling /support command for chat_id %s", common.mask(chat_id))
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    # Check rate limit (1 message every 5 minutes)
    last_support_time = context.user_data.get("last_support_time")
    if last_support_time and (datetime.now() - last_support_time) < timedelta(minutes=5):
        await update.message.reply_text("⏳ Please wait a few minutes before sending another support message.")
        return ConversationHandler.END

    # Initialize support_requests in bot_data if not present
    if "support_requests" not in context.bot_data:
        context.bot_data["support_requests"] = {}

    if context.args:
        message = " ".join(context.args)
        if len(message) < 5:
            await update.message.reply_text("⚠️ Your message is too short. Please provide at least 5 characters.")
            return ConversationHandler.END
        if len(message) > 500:
            await update.message.reply_text("⚠️ Your message is too long. Please keep it under 500 characters.")
            return ConversationHandler.END

        # Animation effect
        transitional_message = await update.message.reply_text("⏳ Sending your feedback...")
        await asyncio.sleep(0.5)
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=transitional_message.message_id)
        except TelegramError as e:
            logger.debug("Failed to delete transitional message for chat_id %s: %s", common.mask(chat_id), str(e))

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

        # Store support request
        request_id = str(int(time.time() * 1000))  # Unique ID based on timestamp
        context.bot_data["support_requests"][request_id] = {
            "chat_id": str(chat_id),
            "message": message,
            "timestamp": datetime.now()
        }

        # Send to admin with Reply button
        try:
            special_chars = r'_*[]()~`>#+-=|{}.!'
            escaped_message = ''.join(f'\\{c}' if c in special_chars else c for c in message)
            await context.bot.send_message(
                chat_id=config.ADMIN_CHAT_ID,
                text=f"📞 *Support Request*\n\nUser Info:\n{user_info}\n\nMessage:\n{escaped_message}",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Reply", callback_data=f"reply_{request_id}")]])
            )
            context.user_data["last_support_time"] = datetime.now()
            await update.message.reply_text("✅ Thank you for your feedback! 📞 We've sent it to our team.")
        except Exception as e:
            logger.error("Failed to send support message for chat_id %s: %s", common.mask(chat_id), str(e))
            await update.message.reply_text("⚠️ Failed to send your message. Please try again later.")
        return ConversationHandler.END
    else:
        await update.message.reply_text("📞 We're listening! Please send your feedback or issue. Use /cancel to stop.")
        return AWAITING_SUPPORT_MESSAGE

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

    # Store support request
    request_id = str(int(time.time() * 1000))  # Unique ID based on timestamp
    context.bot_data["support_requests"][request_id] = {
        "chat_id": str(chat_id),
        "message": message,
        "timestamp": datetime.now()
    }

    # Send to admin with Reply button
    try:
        special_chars = r'_*[]()~`>#+-=|{}.!'
        escaped_message = ''.join(f'\\{c}' if c in special_chars else c for c in message)
        await context.bot.send_message(
            chat_id=config.ADMIN_CHAT_ID,
            text=f"📞 *Support Request*\n\nUser Info:\n{user_info}\n\nMessage:\n{escaped_message}",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Reply", callback_data=f"reply_{request_id}")]])
        )
        context.user_data["last_support_time"] = datetime.now()
        await update.message.reply_text("✅ Thank you for your feedback! 📞 We've sent it to our team.")
    except Exception as e:
        logger.error("Failed to send support message for chat_id %s: %s", common.mask(chat_id), str(e))
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
    """Handles the 'Reply' button click by the admin to initiate a reply conversation."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    if str(chat_id) != config.ADMIN_CHAT_ID:
        logger.warning("Unauthorized reply attempt by chat_id %s", common.mask(chat_id))
        await query.edit_message_text("⚠️ You are not authorized to use this command.")
        return ConversationHandler.END

    request_id = query.data.replace("reply_", "")
    support_request = context.bot_data.get("support_requests", {}).get(request_id)
    if not support_request:
        await query.edit_message_text("⚠️ Support request not found or expired.")
        return ConversationHandler.END

    context.user_data["reply_chat_id"] = support_request["chat_id"]
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=query.message.message_id,
            text=f"📝 Please enter your reply for user {support_request['chat_id']}."
        )
    except TelegramError as e:
        logger.debug("Failed to edit message for chat_id %s: %s", common.mask(chat_id), str(e))
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"📝 Please enter your reply for user {support_request['chat_id']}."
        )
    return AWAITING_ADMIN_REPLY

async def reply_message_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the admin's reply message to the user via callback conversation."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    if str(chat_id) != config.ADMIN_CHAT_ID:
        logger.warning("Unauthorized reply attempt by chat_id %s", common.mask(chat_id))
        await update.message.reply_text("⚠️ You are not authorized to use this command.")
        return ConversationHandler.END

    reply_message = update.message.text
    if len(reply_message) < 5:
        await update.message.reply_text("⚠️ Your reply is too short. Please provide at least 5 characters, or use /cancel to stop.")
        return AWAITING_ADMIN_REPLY
    if len(reply_message) > 4096:
        await update.message.reply_text("⚠️ Your reply is too long. Please keep it under 4096 characters, or use /cancel to stop.")
        return AWAITING_ADMIN_REPLY

    target_chat_id = context.user_data.get("reply_chat_id")
    if not target_chat_id:
        await update.message.reply_text("⚠️ No user selected for reply. Please start over.")
        return ConversationHandler.END

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
        escaped_message = ''.join(f'\\{c}' if c in special_chars else c for c in reply_message)
        await context.bot.send_message(
            chat_id=target_chat_id,
            text=f"📩 *Reply from Support Team*:\n\n{escaped_message}",
            parse_mode="MarkdownV2"
        )
        await update.message.reply_text(f"✅ Reply sent to user {target_chat_id}.")
    except TelegramError as e:
        logger.error("Failed to send reply to chat_id %s: %s", common.mask(target_chat_id), str(e))
        await update.message.reply_text(f"⚠️ Failed to send reply to {target_chat_id}. The user may have blocked the bot or the chat_id is invalid.")

    # Clean up
    context.user_data.pop("reply_chat_id", None)
    return ConversationHandler.END

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation with an animation effect."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    
    # Animation effect
    message = await update.message.reply_text("❌ Cancelling...")
    await asyncio.sleep(0.5)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
    except TelegramError as e:
        logger.debug("Failed to delete transitional message for chat_id %s: %s", common.mask(chat_id), str(e))

    # Clear product selection and reply state
    for key in ["selected_products", "product_menu_view", "product_menu_category", "cached_user", "reply_chat_id"]:
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

    # Animation effect
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
    """Handler for the /broadcast command."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    if str(chat_id) != config.ADMIN_CHAT_ID:
        await update.message.reply_text("⚠️ You are not authorized to use this command.")
        logger.warning("Unauthorized broadcast attempt by chat_id %s", common.mask(chat_id))
        return

    message_to_broadcast = update.message.text[len("/broadcast "):].strip()
    if not message_to_broadcast:
        await update.message.reply_text("⚠️ Please provide a message to broadcast. Usage: /broadcast <message>")
        return

    if len(message_to_broadcast) > 4096:
        await update.message.reply_text("⚠️ Message too long. Please keep it under 4096 characters.")
        return

    context.user_data['broadcast_message'] = message_to_broadcast

    keyboard = [
        [InlineKeyboardButton("✅ Accept", callback_data='broadcast_accept')],
        [InlineKeyboardButton("❌ Reject", callback_data='broadcast_reject')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"📢 You are about to send the following message to all active users:\n\n---\n{message_to_broadcast}\n---\n\nPlease confirm.",
        parse_mode="MarkdownV2",
        reply_markup=reply_markup
    )

async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback handler for broadcast confirmation with animation effect."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    
    if str(chat_id) != config.ADMIN_CHAT_ID:
        logger.warning("Unauthorized broadcast callback interaction by chat_id %s", common.mask(chat_id))
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
        if not message:
            await query.edit_message_text("⚠️ Error: Broadcast message not found. Please try again.")
            return

        # Escape MarkdownV2 special characters
        special_chars = r'_*[]()~`>#+-=|{}.!'
        escaped_message = ''.join(f'\\{c}' if c in special_chars else c for c in message)

        if config.USE_DATABASE:
            all_users = await db.get_all_users()
            active_users = [user for user in all_users if user.get('active')]
        else:
            users_data = context.bot_data.get("users_data", common.read_users_file())
            all_users = users_data.get("users", [])
            active_users = [user for user in all_users if user.get('active')]
        
        sent_count = 0
        for user in active_users:
            try:
                await context.bot.send_message(
                    chat_id=user['chat_id'],
                    text=escaped_message,
                    parse_mode="MarkdownV2"
                )
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error("Failed to send broadcast to chat_id %s: %s", common.mask(user['chat_id']), e)

        await query.edit_message_text(f"✅ Broadcast sent to {sent_count} active users 📢.")
        logger.info("Admin %s sent broadcast to %d users.", common.mask(chat_id), sent_count)
        context.user_data.pop('broadcast_message', None)

    elif query.data == 'broadcast_reject':
        await query.edit_message_text("❌ Broadcast canceled.")
        logger.info("Admin %s canceled broadcast.", common.mask(chat_id))
        context.user_data.pop("broadcast_message", None)

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
            AWAITING_PRODUCT_SELECTION: [CallbackQueryHandler(set_products_callback, pattern='^products_')],
            AWAITING_ADMIN_REPLY: [MessageHandler(filters.TEXT & ~filters.COMMAND, reply_message_received)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_conversation),
            CommandHandler("setproducts", set_products),  # Allow /setproducts to restart
        ],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reply", reply))  # Add direct /reply command
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CallbackQueryHandler(reactivate_callback, pattern='^reactivate$'))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern='^broadcast_'))

    asyncio.run(run_polling(app))

if __name__ == "__main__":
    main()