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
from database import Database

logger = common.setup_logging()
logger.setLevel(logging.DEBUG)

# Initialize database
db = None

# Conversation states
AWAITING_PINCODE, AWAITING_SUPPORT_MESSAGE, AWAITING_PRODUCT_SELECTION, AWAITING_ADMIN_REPLY, AWAITING_NOTIFICATION_PREFERENCE = range(5)

# Simple escape_markdown function (from prev_main.py)
def escape_markdown(text):
    """Escape special characters for MarkdownV2."""
    special_chars = r'_*[]()~`>#+-=|{}.!-'
    return ''.join(f'\\{c}' if c in special_chars else c for c in text)

async def notification_preference(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /notification_preference command."""
    chat_id = update.effective_chat.id
    logger.info("Handling /notification_preference command for chat_id %s", chat_id)
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    user = await db.get_user(chat_id)
    if not user:
        await update.message.reply_text("*⚠️ You need to register first*. Use /setpincode to begin.", parse_mode="Markdown")
        return

    # Clear any stale product selection states
    for key in [k for k in context.user_data.keys() if k.startswith("product_menu_") or k == "selected_products"]:
        context.user_data.pop(key, None)

    current_preference = user.get("notification_preference", "until_stop")

    keyboard = [
        [InlineKeyboardButton("🔔 Notify once and stop", callback_data="notif_pref_once_and_stop")],
        [InlineKeyboardButton("🔄 Notify once per restock", callback_data="notif_pref_once_per_restock")],
        [InlineKeyboardButton("♾️ Notify until /stop", callback_data="notif_pref_until_stop")],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    preference_names = {
        "once_and_stop": "🔔 Notify once and stop",
        "once_per_restock": "🔄 Notify once per restock", 
        "until_stop": "♾️ Notify until /stop"
    }

    current_name = preference_names.get(current_preference, "Unknown")

    message_text = (
        "🔔 *Notification Preferences*\n\n"
        f"*Current setting*: {current_name}\n\n"
        "*Available Options:*\n\n"
        "🔔 *Notify once and stop* - Get notified once when product is available, then stop notifications for that product\n\n"
        "🔄 *Notify once per restock* - Get notified once each time the product goes from out-of-stock to in-stock\n\n"
        "♾️ *Notify until /stop* - Keep getting notifications while the product is available\n\n"
        "*Choose your preference*:"
    )

    await update.message.reply_text(
        message_text,
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def notification_preference_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle notification preference selection."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id

    preference_map = {
        "notif_pref_once_and_stop": "once_and_stop",
        "notif_pref_once_per_restock": "once_per_restock",
        "notif_pref_until_stop": "until_stop"
    }

    new_preference = preference_map.get(query.data)
    if not new_preference:
        await query.edit_message_text("⚠️ Invalid preference selected.")
        return

    user = await db.get_user(chat_id)
    if not user:
        await query.edit_message_text("⚠️ User not found. Please use /start to register.")
        return

    try:
        # Reset last_notified when changing preference
        if user.get("notification_preference") != new_preference:
            user["last_notified"] = {}
        
        user["notification_preference"] = new_preference
        await db.update_user(chat_id, user)
        await db.commit()

        preference_names = {
            "once_and_stop": "🔔 Notify once and stop",
            "once_per_restock": "🔄 Notify once per restock",
            "until_stop": "♾️ Notify until /stop"
        }

        selected_name = preference_names[new_preference]
        message_text = f"✅ Notification preference updated to: \n{selected_name}"
        await query.edit_message_text(message_text)
        logger.info(f"Updated notification preference for chat_id {chat_id} to {new_preference}")

    except Exception as e:
        await db.rollback()
        logger.error(f"Error updating notification preference for chat_id {chat_id}: {str(e)}")
        await query.edit_message_text("⚠️ Failed to update notification preference. Please try again.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /start command."""
    chat_id = update.effective_chat.id
    logger.info("Handling /start command for chat_id %s", chat_id)
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    user = await db.get_user(chat_id)

    if user and user.get("pincode"):
        pincode = user.get("pincode")
        if user.get("active"):
            products = user.get("products", ["Any"])
            product_message = "All of the available Amul Protein products 🧀" if len(products) == 1 and products[0].lower() == "any" else "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in products)

            message_text = (
                f"🎉 You have already enabled notifications for PINCODE {pincode} 📍.\n\n"
                f"You are currently tracking:\n{product_message}"
            )
        else:
            user["active"] = True
            await db.update_user(chat_id, user)
            await db.commit()

            message = await update.message.reply_text("⏳ Re-enabling notifications...")
            await asyncio.sleep(0.5)

            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
            except TelegramError as e:
                logger.debug("Failed to delete transitional message for chat_id %s: %s", chat_id, str(e))

            message_text = (
                f"🎉 Welcome back! Notifications have been re-enabled for PINCODE {pincode} 📍.\n"
                "Use /stop to pause them again."
            )
    else:
        message_text = (
            "👋 Welcome to the Amul Protein Items Notifier Bot! 🧀\n\n"
            "Use /setpincode PINCODE to set your pincode 📍 (Mandatory).\n"
            "Use /setproducts to select products 🧀 (Optional, defaults to any Amul protein product).\n"
            "Use /notification_preference to change how you are notified about product availability 🔔.\n"
            "Use /my_settings to view your current pincode and product/notification related config.\n"
            "Use /support to report issues or support the project 📞."
        )

    # Escape the entire message for MarkdownV2
    escaped_text = escape_markdown(message_text)

    await update.message.reply_text(escaped_text, parse_mode="MarkdownV2")


async def _save_pincode(chat_id: int, pincode: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Helper function to save the pincode for a user using database only."""
    try:
        user = await db.get_user(chat_id)
        if user:
            user["pincode"] = pincode
            user["active"] = True
            user["last_notified"] = {}
            await db.update_user(chat_id, user)
        else:
            new_user = {"chat_id": str(chat_id), 
                        "pincode": pincode, 
                        "products": ["Any"], 
                        "active": True,
                        "last_notified" : {}}
            await db.add_user(chat_id, new_user)
        await db.commit()
        return True
    except Exception as e:
        await db.rollback()
        logger.error(f"Error saving pincode for chat_id {chat_id}: {str(e)}")
        return False

async def set_pincode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the conversation to set a pincode or sets it directly if provided."""
    chat_id = update.effective_chat.id
    logger.info("Handling /setpincode command for chat_id %s", chat_id)
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
            logger.debug("Failed to delete transitional message for chat_id %s: %s", chat_id, str(e))

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
    logger.info("Handling /support command for chat_id %s", chat_id)
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    user = await db.get_user(chat_id)
    if not user:
        await update.message.reply_text("*⚠️ You need to register first*. Use /setpincode to begin.", parse_mode="Markdown")
        return

    # Clear stale product selection states
    for key in [k for k in context.user_data.keys() if k.startswith("product_menu_") or k == "selected_products"]:
        context.user_data.pop(key, None)

    keyboard = [
        [InlineKeyboardButton("Contact Me 📞", callback_data="support_contact")],
        [InlineKeyboardButton("Support Project 🌟", callback_data="support_project")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("📞 How can we assist you today?", reply_markup=reply_markup)
    return AWAITING_PRODUCT_SELECTION

async def support_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the support menu callback actions."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    logger.debug("Support callback triggered for chat_id %s with action %s", chat_id, query.data)
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    action = query.data

    if action == "support_contact":
        # Check rate limit (1 message every 1 minute)
        last_support_time = context.user_data.get("last_support_time")
        if last_support_time and (datetime.now() - last_support_time) < timedelta(minutes=1):
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
        await query.edit_message_text("😊 Thank you for the tip! Please support via https://razorpay.me/@amulproteinnotifierbot", reply_markup=reply_markup)
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

    # Check rate limit (aligned to 1 minute for consistency)
    last_support_time = context.user_data.get("last_support_time")
    if last_support_time and (datetime.now() - last_support_time) < timedelta(minutes=1):
        await update.message.reply_text("⏳ Please wait a few minutes before sending another support message.")
        return ConversationHandler.END

    if len(message) < 5:
        await update.message.reply_text("⚠️ Your message is too short. If this was a mistake, use /cancel to stop.")
        return AWAITING_SUPPORT_MESSAGE

    if len(message) > 500:
        await update.message.reply_text("⚠️ Your message is too long. Please keep it under 500 characters, or use /cancel to stop.")
        return AWAITING_SUPPORT_MESSAGE

    # Log the support message
    logger.info("User chat_id %s sent support message: %s", chat_id, message)

    # Initialize support_requests in bot_data if not present
    if "support_requests" not in context.bot_data:
        context.bot_data["support_requests"] = {}

    # Get user data from database
    user = await db.get_user(chat_id)

    # Prepare user info for admin
    user_info = f"Chat ID: {chat_id}\n"
    user_info += f"Pincode: {user.get('pincode', 'Not set')}\n"
    products = user.get("products", ["Any"]) if user else ["Any"]
    product_message = "All available Amul Protein products" if len(products) == 1 and products[0].lower() == "any" else "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in products)
    user_info += f"Tracked Products:\n{product_message}\n"

    # Add notification preference
    notification_preference = user.get("notification_preference", "until_stop")
    preference_names = {
        "once_and_stop": "🔔 Notify once and stop",
        "once_per_restock": "🔄 Notify once per restock",
        "until_stop": "♾️ Notify until /stop"
    }
    preference_name = preference_names.get(notification_preference, "Unknown")
    user_info += f"Notification Preference: {preference_name}\n"

    # Escape the user's message and user_info
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
    """Handles the /reply command for the admin to send a message to any user."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    if str(chat_id) != config.ADMIN_CHAT_ID:
        logger.warning("Unauthorized reply attempt by chat_id %s", chat_id)
        await update.message.reply_text("⚠️ You are not authorized to use this command.")
        return

    # Get the full message text
    full_text = update.message.text
    if not full_text or len(full_text) <= len("/reply"):
        await update.message.reply_text("⚠️ Usage: /reply <chat_id> <message>")
        return

    # Parse chat_id and message
    parts = full_text[len("/reply"):].strip().split(maxsplit=1)
    if len(parts) < 2 or not parts[0].strip():
        await update.message.reply_text("⚠️ Usage: /reply <chat_id> <message>")
        return

    target_chat_id = parts[0].strip()
    message = parts[1].strip() if len(parts) > 1 else ""

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
        logger.debug("Failed to delete transitional message for chat_id %s: %s", chat_id, str(e))

    # Escape special characters for MarkdownV2
    escaped_message = escape_markdown(message)

    # Send reply to user with MarkdownV2 formatting, preserving newlines
    try:
        await context.bot.send_message(
            chat_id=target_chat_id,
            text=f"📩 *Response from Admin*:\n\n{escaped_message}",
            parse_mode="MarkdownV2"
        )

        await update.message.reply_text(f"✅ Reply sent to user {target_chat_id}.")
    except TelegramError as e:
        logger.error("Failed to send reply to chat_id %s: %s", target_chat_id, str(e))
        await update.message.reply_text(f"⚠️ Failed to send reply to {target_chat_id}. The user may have blocked the bot or the chat_id is invalid.")

async def reply_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the reply action for a support request."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    # Clear stale product selection states
    for key in [k for k in context.user_data.keys() if k.startswith("product_menu_") or k == "selected_products"]:
        context.user_data.pop(key, None)

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

    # Escape special characters for MarkdownV2
    escaped_message = escape_markdown(message)

    try:
        await context.bot.send_message(
            chat_id=user_chat_id,
            text=f"📩 *Response from Admin*:\n\n{escaped_message}",
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
        logger.debug("Failed to delete transitional message for chat_id %s: %s", chat_id, str(e))

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
    logger.info("Starting product selection for chat_id %s", chat_id)
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    start_time = time.time()

    # Clear previous product selection state
    for key in ["selected_products", "product_menu_view", "product_menu_category", "cached_user"]:
        context.user_data.pop(key, None)

    user = await db.get_user(chat_id)
    if not user:
        await update.message.reply_text("*⚠️ You need to register first*. Use /setpincode to begin.", parse_mode="Markdown")
        return

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
        "🧀 Please select the products you want to monitor.", 
        reply_markup=reply_markup
    )

    logger.info("set_products took %.2f seconds for chat_id %s", time.time() - start_time, chat_id)
    return AWAITING_PRODUCT_SELECTION

async def set_products_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles all interactions for the product selection menu with animation effects."""

    start_time = time.time()
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    logger.debug("set_products_callback called with chat_id: %s from update: %s", chat_id, update.effective_chat.id)

    # Validate chat_id against the update context
    if chat_id != update.effective_chat.id:
        logger.warning("Chat_id mismatch: query=%s, update=%s. Ending conversation.", chat_id, update.effective_chat.id)
        await query.answer("Session expired. Use /setproducts to restart.")
        return ConversationHandler.END

    # Check if user is active
    user = await db.get_user(chat_id)
    if not user or not user.get("active", False):
        logger.warning("Inactive or non-existent user for chat_id %s", chat_id)
        await query.answer("User inactive or not found. Use /start to reactivate.")
        return ConversationHandler.END

    try:
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
        # Ensure selected_products is initialized
        if "selected_products" not in context.user_data:
            context.user_data["selected_products"] = set()

        selected_products = context.user_data["selected_products"]
        logger.debug("Selected products for chat_id %s: %s", chat_id, selected_products)

        action = query.data
        action_for_rendering = action

        if action == "products_nav_main" and selected_products:
            display_products = [common.PRODUCT_NAME_MAP.get(p, p) for p in selected_products if p in common.PRODUCTS]
            product_list_text = "\n".join(f"- {p}" for p in display_products if p)

            if not product_list_text:
                product_list_text = "No valid products selected."
                logger.warning("No valid products in display_products for chat_id %s: %s", chat_id, display_products)

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
            logger.info("Confirmation menu rendered in %.2f seconds for chat_id %s", time.time() - start_time, chat_id)
            return AWAITING_PRODUCT_SELECTION

        if action.startswith("products_toggle_"):
            try:
                product_index = int(action.replace("products_toggle_", ""))
                if product_index < 0 or product_index >= len(common.PRODUCTS):
                    logger.error("Invalid product_index %d for chat_id %s", product_index, chat_id)
                    await query.edit_message_text("⚠️ Invalid product selection. Please try again or use /setproducts to restart.")
                    return AWAITING_PRODUCT_SELECTION

                product_name = common.PRODUCTS[product_index]
                if product_name in selected_products:
                    selected_products.remove(product_name)
                else:
                    selected_products.add(product_name)

                logger.debug("Toggled product %s for chat_id %s, new selected_products: %s", product_name, chat_id, selected_products)

            except ValueError:
                logger.error("Invalid product_toggle action for chat_id %s: %s", chat_id, action)
                await query.edit_message_text("⚠️ Invalid product selection. Please try again or use /setproducts to restart.")
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
                        selected_marker = "☑️ " if product_name in selected_products else ""
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
                        selected_marker = "☑️ " if product_name in selected_products else ""
                        keyboard.append([InlineKeyboardButton(f"{selected_marker}{common.PRODUCT_NAME_MAP.get(product_name, product_name)}", callback_data=f"products_toggle_{i}")])

                    keyboard.append([
                        InlineKeyboardButton("✅ Confirm Selection", callback_data="products_confirm"),
                        InlineKeyboardButton("❌ Clear Selection", callback_data="products_clear"),
                    ])
                    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="products_nav_main")])

                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(text=text, reply_markup=reply_markup)
                logger.info("Clear selection (no products) took %.2f seconds for chat_id %s", time.time() - start_time, chat_id)
                return AWAITING_PRODUCT_SELECTION

            if current_category:
                products_to_clear = set(common.CATEGORIZED_PRODUCTS[current_category])
                selected_products -= products_to_clear
            else:
                selected_products.clear()

            logger.debug("Cleared products for chat_id %s, new selected_products: %s", chat_id, selected_products)
            action_for_rendering = f"products_view_cat_{current_category}" if current_category else "products_nav_all"

        elif action == "products_confirm_Any":
            final_selection = ["Any"]
            user = context.user_data.get("cached_user", {})
            user["products"] = final_selection
            user["active"] = True
            user["last_notified"] = {}  # Reset notification timestamps!
            logger.debug("Confirm Any for chat_id %s, final_selection: %s", chat_id, final_selection)

            try:
                await query.edit_message_text("✅ Saving your selection...")
                await asyncio.sleep(0.5)
            except TelegramError as e:
                logger.debug("Failed to edit message for chat_id %s: %s", chat_id, str(e))

            try:
                await db.update_user(chat_id, user)
                await db.commit()
                await query.edit_message_text("✅ Your selection has been saved. \nYou will be notified if any of the Amul Protein product is available❗.")
            except Exception as e:
                await db.rollback()
                logger.error("Database error for chat_id %s: %s", chat_id, str(e))
                await query.edit_message_text("⚠️ Failed to save your selection. Please try again later.")

            for key in [key for key in context.user_data if key.startswith("product_menu_")]:
                del context.user_data[key]
            context.user_data["selected_products"] = set()

            logger.info("Confirm Any took %.2f seconds for chat_id %s", time.time() - start_time, chat_id)
            return ConversationHandler.END  # End conversation after confirmation

        elif action == "products_confirm":
            if not selected_products:
                user = context.user_data.get("cached_user", {})
                current_tracked_products = user.get("products", ["Any"])
                product_message = "All of the available Amul Protein products 🧀" if len(current_tracked_products) == 1 and current_tracked_products[0].lower() == "any" else "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in current_tracked_products)

                await query.edit_message_text(f"⚠️ No products were selected. You are currently tracking:\n{product_message}")

                for key in [key for key in context.user_data if key.startswith("product_menu_")]:
                    del context.user_data[key]
                context.user_data["selected_products"] = set()

                logger.info("Confirm (no selection) took %.2f seconds for chat_id %s", time.time() - start_time, chat_id)
                return ConversationHandler.END  # End conversation after no selection

            final_selection = ["Any"] if "Any" in selected_products else list(selected_products)
            user = context.user_data.get("cached_user", {})
            user["products"] = final_selection
            user["active"] = True
            user["last_notified"] = {}  # Reset notification timestamps!
            product_message = "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in final_selection if common.PRODUCT_NAME_MAP.get(p, p))

            if not product_message:
                product_message = "No valid product names available."
                logger.warning("Empty product message for chat_id %s, final_selection: %s", chat_id, final_selection)

            logger.debug("Confirm selection for chat_id %s, final_selection: %s", chat_id, final_selection)

            try:
                await query.edit_message_text("✅ Saving your selection...")
                await asyncio.sleep(0.5)
            except TelegramError as e:
                logger.debug("Failed to edit message for chat_id %s: %s", chat_id, str(e))

            try:
                await db.update_user(chat_id, user)
                await db.commit()
                await query.edit_message_text(f"✅ Your selections have been saved. You will be notified for:\n{product_message}")
            except Exception as e:
                await db.rollback()
                logger.error("Database error for chat_id %s: %s", chat_id, str(e))
                await query.edit_message_text("⚠️ Failed to save your selections. Please try again later.")

            for key in [key for key in context.user_data if key.startswith("product_menu_")]:
                del context.user_data[key]
            context.user_data["selected_products"] = set()

            logger.info("Confirm selection took %.2f seconds for chat_id %s", time.time() - start_time, chat_id)
            return ConversationHandler.END  # End conversation after confirmation

        elif action == "products_clear_and_back_to_main":
            try:
                await query.edit_message_text("❌ Clearing selection...")
                await asyncio.sleep(0.5)
            except TelegramError as e:
                logger.debug("Failed to edit message for chat_id %s: %s", chat_id, str(e))

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
                selected_marker = "☑️ " if product_name in selected_products else ""
                keyboard.append([InlineKeyboardButton(f"{selected_marker}{common.PRODUCT_NAME_MAP.get(product_name, product_name)}", callback_data=f"products_toggle_{product_index}")])

        elif action_for_rendering == "products_nav_all":
            context.user_data["product_menu_view"] = "all"
            text = "🧀 Select products to monitor:"

            for i, product_name in enumerate(common.PRODUCTS):
                if product_name == "Any": continue
                selected_marker = "☑️ " if product_name in selected_products else ""
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

        logger.info("Menu rendering took %.2f seconds for chat_id %s", time.time() - start_time, chat_id)
        return AWAITING_PRODUCT_SELECTION

    except Exception as e:
        logger.error("Error in set_products_callback for chat_id %s: %s", chat_id, str(e))
        await query.edit_message_text("⚠️ An error occurred. Please try again or use /setproducts to restart.")

        for key in [key for key in context.user_data if key.startswith("product_menu_")]:
            del context.user_data[key]
        context.user_data["selected_products"] = set()

        logger.info("Error handling took %.2f seconds for chat_id %s", time.time() - start_time, chat_id)
        return ConversationHandler.END  # End conversation on error

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Deactivates notifications for the user."""
    chat_id = update.effective_chat.id
    logger.info("Handling /stop command for chat_id %s", chat_id)
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    user = await db.get_user(chat_id)
    if user:
        if user.get("active"):
            try:
                user["active"] = False
                await db.update_user(chat_id, user)
                await db.commit()

                keyboard = [[InlineKeyboardButton("🔄 Re-activate", callback_data="reactivate")]]
                reply_markup = InlineKeyboardMarkup(keyboard)

                message_text = "❌ Notifications have been disabled. Use /start or click Re-activate button to enable again."
                escaped_text = escape_markdown(message_text)
                await update.message.reply_text(escaped_text, reply_markup=reply_markup, parse_mode="MarkdownV2")
            except Exception as e:
                await db.rollback()
                logger.error(f"Error deactivating notifications for chat_id {chat_id}: {str(e)}")
                await update.message.reply_text("⚠️ Failed to deactivate notifications. Please try again.")
        else:
            await update.message.reply_text("ℹ️ Notifications are already disabled. Use /start to enable them.")
    else:
        await update.message.reply_text("ℹ️ You are not registered. Use /setpincode to begin.")

async def reactivate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the reactivation callback."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id

    user = await db.get_user(chat_id)
    if user:
        try:
            user["active"] = True
            await db.update_user(chat_id, user)
            await db.commit()

            message_text = "✅ Notifications have been re-enabled! 🔔"
            escaped_text = escape_markdown(message_text)
            await query.edit_message_text(escaped_text, parse_mode="MarkdownV2")
        except Exception as e:
            await db.rollback()
            logger.error(f"Error reactivating notifications for chat_id {chat_id}: {str(e)}")
            await query.edit_message_text("⚠️ Failed to reactivate notifications. Please try again.")
    else:
        await query.edit_message_text("⚠️ User not found. Please use /start to register.")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the current status and settings for the user."""
    chat_id = update.effective_chat.id
    logger.info("Handling /status command for chat_id %s", chat_id)
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    user = await db.get_user(chat_id)
    if user:
        pincode = user.get("pincode", "Not set")
        products = user.get("products", ["Any"])
        active = user.get("active", False)
        notification_preference = user.get("notification_preference", "until_stop")

        preference_names = {
            "once_and_stop": "🔔 Notify once and stop",
            "once_per_restock": "🔄 Notify once per restock",
            "until_stop": "♾️ Notify until /stop"
        }

        preference_name = preference_names.get(notification_preference, "Unknown")

        status_text = f"📊 *Your Current Settings*:\n\n"
        status_text += f"📍 *Pincode*: {pincode}\n\n"
        status_text += f"🔔 *Notifications*: {'✅ Enabled' if active else '❌ Disabled'}\n"
        status_text += f"⚙️ *Notification Preference*: {preference_name}\n\n"
        status_text += f"🧀 *Tracked Products*:\n"

        if len(products) == 1 and products[0].lower() == "any":
            status_text += "- All available Amul Protein products 🧀"
        else:
            for product in products:
                status_text += f"- {common.PRODUCT_NAME_MAP.get(product, product)}\n"

        await update.message.reply_text(status_text, parse_mode="Markdown")
    else:
        await update.message.reply_text("ℹ️ You are not registered. Use /setpincode to begin.")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcasts a message to all users or specific groups (Admin only)."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    if str(chat_id) != config.ADMIN_CHAT_ID:
        logger.warning("Unauthorized broadcast attempt by chat_id %s", chat_id)
        await update.message.reply_text("⚠️ You are not authorized to use this command.")
        return

    # Get the full message text
    full_text = update.message.text
    if not full_text or len(full_text) <= len("/broadcast"):
        await update.message.reply_text("⚠️ Usage: /broadcast [all|active|inactive] <message>")
        return

    # FIXED: Parse target group and message with maxsplit=1
    after_broadcast = full_text[len("/broadcast"):].strip()
    parts = after_broadcast.split(maxsplit=1)  # Only split once to separate target from message
    
    target_group = "active"  # Default target group
    message_to_broadcast = ""

    if len(parts) >= 2:
        potential_target = parts[0].lower().strip()
        if potential_target in ["all", "active", "inactive"]:
            target_group = potential_target
            message_to_broadcast = parts[1].strip()  # Everything after the target
        else:
            # If first word is not a valid target, treat entire text as message
            message_to_broadcast = after_broadcast.strip()
    elif len(parts) == 1:
        # Check if the single part is a target or message
        single_part = parts[0].lower().strip()
        if single_part in ["all", "active", "inactive"]:
            target_group = single_part
            message_to_broadcast = ""  # No message provided
        else:
            message_to_broadcast = parts[0].strip()

    if not message_to_broadcast:
        await update.message.reply_text("⚠️ Please provide a message to broadcast.")
        return

    if len(message_to_broadcast) > 4096:
        await update.message.reply_text("⚠️ Message too long. Please keep it under 4096 characters.")
        return

    # Animation effect
    transitional_message = await update.message.reply_text("⏳ Preparing broadcast...")
    await asyncio.sleep(0.5)

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=transitional_message.message_id)
    except TelegramError as e:
        logger.debug("Failed to delete transitional message for chat_id %s: %s", chat_id, str(e))

    # Get users from database
    all_users = await db.get_all_users()

    # Filter users based on target group
    if target_group == "all":
        target_users = all_users
    elif target_group == "active":
        target_users = [user for user in all_users if user.get("active", False)]
    elif target_group == "inactive":
        target_users = [user for user in all_users if not user.get("active", False)]
    else:
        target_users = []

    if not target_users:
        await update.message.reply_text(f"⚠️ No {target_group} users found to broadcast to.")
        return

    # Show confirmation menu
    keyboard = [
        [InlineKeyboardButton("✅ Accept", callback_data='broadcast_accept')],
        [InlineKeyboardButton("❌ Reject", callback_data='broadcast_reject')],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    context.user_data['broadcast_message'] = message_to_broadcast
    context.user_data['broadcast_target'] = target_group
    context.user_data['broadcast_target_users'] = target_users  # Store users for callback

    # Use the same approach as prev_main.py - escape the full message for display
    base_text = f"📢 You are about to send the following message to {target_group} users:\n\n---\n{message_to_broadcast}\n---\n\nPlease confirm."
    escaped_full_text = escape_markdown(base_text)

    await update.message.reply_text(
        escaped_full_text,
        parse_mode="MarkdownV2",
        reply_markup=reply_markup
    )

async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles broadcast confirmation callbacks."""
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
        logger.debug("Failed to edit transitional message for chat_id %s: %s", chat_id, str(e))

    if query.data == 'broadcast_accept':
        message = context.user_data.get('broadcast_message')
        target_group = context.user_data.get('broadcast_target')
        target_users = context.user_data.get('broadcast_target_users')  # Retrieve stored users

        if not message or not target_users:
            await query.edit_message_text("⚠️ Error: Broadcast data not found. Please try again.")
            return

        # Escape the broadcast message using the same approach as prev_main.py
        escaped_message = escape_markdown(message)

        sent_count = 0
        delay = 0.1  # Delay between sends to respect Telegram API limits
        
        # NEW: Record start time here
        start_time = time.time()
        admin_chat_id = chat_id
        
        for user in target_users:
            try:
                user_chat_id = int(user['chat_id'])
                context.job_queue.run_once(
                    send_broadcast_job,
                    when=delay * sent_count,
                    # FIXED: Pass only the escaped message (no prefix here)
                    data={"chat_id": user_chat_id, "message": escaped_message}
                )
                sent_count += 1
            except Exception as e:
                logger.warning(f"Failed to queue broadcast for user {user.get('chat_id')}: {str(e)}")
        
        # NEW: Queue completion job with a buffer after the last broadcast
        completion_delay = delay * (sent_count + 1)  # Buffer to ensure all jobs finish
        context.job_queue.run_once(
            send_broadcast_completion,
            when=completion_delay,
            data={
                "start_time": start_time,
                "admin_chat_id": admin_chat_id,
                "sent_count": sent_count,
                "target_group": target_group
            }
        )

        await query.edit_message_text(f"✅ Broadcast queued for {sent_count} {target_group} users 📢. It will be sent in the background.")
        logger.info("Admin %s queued broadcast to %d %s users.", chat_id, sent_count, target_group)

        # Clear broadcast data
        context.user_data.pop('broadcast_message', None)
        context.user_data.pop('broadcast_target', None)
        context.user_data.pop('broadcast_target_users', None)

    elif query.data == 'broadcast_reject':
        await query.edit_message_text("❌ Broadcast canceled.")
        logger.info("Admin %s canceled broadcast.", chat_id)
        # Clear broadcast data
        context.user_data.pop("broadcast_message", None)
        context.user_data.pop("broadcast_target", None)
        context.user_data.pop("broadcast_target_users", None)

async def send_broadcast_job(context: ContextTypes.DEFAULT_TYPE):
    """Job queue function to send a single broadcast message asynchronously."""
    job_data = context.job.data
    chat_id = job_data["chat_id"]
    message = job_data["message"]
    try:
        # FIXED: Add the prefix only here (once) to avoid duplication
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"📢 *Broadcast Message*:\n\n{message}",
            parse_mode="MarkdownV2"
        )
        logger.info("Broadcast sent to chat_id %s", chat_id)
    except Exception as e:
        logger.error("Failed to send broadcast to chat_id %s: %s", chat_id, str(e))

async def send_broadcast_completion(context: ContextTypes.DEFAULT_TYPE):
    """Job queue function to send a completion message after all broadcasts."""
    job_data = context.job.data
    end_time = time.time()
    start_time = job_data["start_time"]
    duration = end_time - start_time
    admin_chat_id = job_data["admin_chat_id"]
    sent_count = job_data["sent_count"]
    target_group = job_data["target_group"]
    
    # Construct the raw message
    raw_message = f"✅ Broadcast completed successfully! It took {duration:.2f} seconds for {sent_count} {target_group} users."
    
    # Escape for MarkdownV2 to fix the '!' issue
    escaped_message = escape_markdown(raw_message)
    
    try:
        await context.bot.send_message(
            chat_id=admin_chat_id,
            text=escaped_message,
            parse_mode="MarkdownV2"
        )
        logger.info("Broadcast completion message sent to admin %s", admin_chat_id)
    except Exception as e:
        logger.error("Failed to send broadcast completion message to admin %s: %s", admin_chat_id, str(e))

async def bot_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows bot statistics (Admin only)."""
    chat_id = update.effective_chat.id
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

    if str(chat_id) != config.ADMIN_CHAT_ID:
        logger.warning("Unauthorized bot_stats attempt by chat_id %s", chat_id)
        await update.message.reply_text("⚠️ You are not authorized to use this command.")
        return

    # Get all users from database
    all_users = await db.get_all_users()

    if not all_users:
        await update.message.reply_text("📊 *No users* found in the database.", parse_mode="Markdown")
        return

    # Calculate statistics (union from both files)
    total_users = len(all_users)
    active_users = sum(1 for user in all_users if user.get("active", False))
    inactive_users = total_users - active_users

    # Notification Preferences (from main.py)
    pref_counts = {"until_stop": 0, "once_and_stop": 0, "once_per_restock": 0}
    for user in all_users:
        pref = user.get("notification_preference", "until_stop")
        if pref in pref_counts:
            pref_counts[pref] += 1

    # Total Unique Pincodes (from prev_main.py)
    distinct_pincodes = len(set(user.get("pincode") for user in all_users if user.get("pincode")))

    # Top 3 States (from prev_main.py, using substore_info)
    pincode_to_state = {}
    for state_data in substore_info:
        state = state_data["name"]
        if state_data["pincodes"]:
            pincodes = state_data["pincodes"].split(",")
            for pincode in pincodes:
                pincode_to_state[pincode.strip()] = state

    user_states = [pincode_to_state.get(user.get("pincode", "").strip(), "Unknown") for user in all_users]
    known_user_states = [state for state in user_states if state != "Unknown"]
    state_counts = Counter(known_user_states)
    top_states = state_counts.most_common(3)

    # Top 5 Products (from main.py, including "Any")
    product_counter = Counter()
    for user in all_users:
        products = user.get("products", ["Any"])
        for product in products:
            product_counter[product] += 1
    top_products_all = product_counter.most_common(5)

    # Top 3 Specific Tracked Products (from prev_main.py, excluding "Any")
    specific_product_counts = Counter()
    for user in all_users:
        products = user.get("products", [])
        if products != ["Any"]:
            specific_product_counts.update(products)
    top_specific_products = specific_product_counts.most_common(3)

    # Users Not Tracking Specific Products (from prev_main.py)
    users_not_tracking_specific = len([user for user in all_users if user.get("products") == ["Any"] or not user.get("products")])

    # Total Support Requests (from prev_main.py, uncommented)
    total_support_requests = len(context.bot_data.get("support_requests", {}))

    # Pincode Distribution (Top 5, from both)
    pincode_counter = Counter()
    for user in all_users:
        pincode = user.get("pincode")
        if pincode:
            pincode_counter[pincode] += 1

    # Build statistics message with improved readability
    stats_message = (
        "📊 *Bot Statistics*\n\n"
        "*User Overview*:\n"
        f"👥 *Total Users*: {total_users}\n"
        f"✅ *Active Users*: {active_users}\n"
        f"❌ *Inactive Users*: {inactive_users}\n\n"
        "*Notification Preferences*:\n"
        f"♾️ *Until Stop*: {pref_counts['until_stop']}\n"
        f"🔔 *Once and Stop*: {pref_counts['once_and_stop']}\n"
        f"🔄 *Once per Restock*: {pref_counts['once_per_restock']}\n\n"
        f"🏙️ *Total Unique Pincodes*: {distinct_pincodes}\n\n"
        "*Top 3 States*:\n"
    )

    for state, count in top_states:
        stats_message += f"- {state}: {count} users\n"

    stats_message += "\n*Top 5 Products (Including 'Any')*:\n"
    for product, count in top_products_all:
        product_name = common.PRODUCT_NAME_MAP.get(product, product)
        stats_message += f"- {product_name}: {count} users\n"

    stats_message += "\n*Top 3 Specific Tracked Products (Excluding 'Any')*:\n"
    if top_specific_products:
        for product, count in top_specific_products:
            product_name = common.PRODUCT_NAME_MAP.get(product, product)
            stats_message += f"- {product_name}: {count} users\n"
    else:
        stats_message += "- No specific products are currently tracked.\n"

    stats_message += (
        f"\n📦 *Users Not Tracking Specific Products*: {users_not_tracking_specific}\n"
        f"📞 *Total Support Requests*: {total_support_requests}\n\n"
        "*Top 5 Pincodes*:\n"
    )

    for pincode, count in pincode_counter.most_common(5):
        stats_message += f"- 📍 {pincode}: {count} users\n"

    await update.message.reply_text(stats_message, parse_mode="Markdown")


async def run_polling(app: Application):
    """Starts the bot in polling mode."""
    global db
    db = Database(config.DATABASE_FILE)
    await db._init_db()

    # Clear all user data on startup to prevent stale states
    for chat_data in app.chat_data.values():
        for key in [k for k in chat_data.keys() if k.startswith("product_menu_") or k == "selected_products"]:
            chat_data.pop(key, None)
    for user_data in app.user_data.values():
        for key in [k for k in user_data.keys() if k.startswith("product_menu_") or k == "selected_products"]:
            user_data.pop(key, None)

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
        if db:
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
            AWAITING_NOTIFICATION_PREFERENCE: [CallbackQueryHandler(notification_preference_callback, pattern='^notif_pref_')],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_conversation),
            CommandHandler("support", support),
            CommandHandler("setproducts", set_products)  # Allow restarting conversations
        ],
        per_message=False,  # Suppresses PTB warnings
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("notification_preference", notification_preference))
    app.add_handler(CallbackQueryHandler(notification_preference_callback, pattern="^notif_pref_"))
    app.add_handler(CommandHandler("reply", reply))  # Add direct /reply command
    app.add_handler(CallbackQueryHandler(cancel_reply_callback, pattern='^cancel_reply_'))
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CallbackQueryHandler(reactivate_callback, pattern='^reactivate$'))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern='^broadcast_'))
    app.add_handler(CommandHandler("broadcast", broadcast))  # Add broadcast command handler
    app.add_handler(CommandHandler("bot_stats", bot_stats))
    app.add_handler(CommandHandler("my_settings", status))

    asyncio.run(run_polling(app))

if __name__ == "__main__":
    main()
