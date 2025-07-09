import asyncio
import json
import base64
import requests
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)

# Local imports
import common
import config
if config.USE_DATABASE:
    from database import Database


logger = common.setup_logging()

if config.USE_DATABASE:
    db = Database(config.DATABASE_FILE)

# Conversation states
AWAITING_PINCODE = 1

def update_users_file(users_data):
    """Update the users.json file in the GitHub repository."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            sha = common.get_file_sha(config.USERS_FILE)
            if not sha:
                logger.error(
                    "Failed to get SHA for %s on attempt %d",
                    config.USERS_FILE,
                    attempt + 1,
                )
                continue

            url = f"https://api.github.com/repos/{config.PRIVATE_REPO}/contents/{config.USERS_FILE}"
            headers = {
                "Authorization": f"token {config.GH_PAT}",
                "Accept": "application/vnd.github+json",
            }
            content = base64.b64encode(
                json.dumps(users_data, indent=2).encode()
            ).decode()
            data = {
                "message": "Update users.json with new user data",
                "content": content,
                "sha": sha,
                "branch": config.GITHUB_BRANCH,
            }

            response = requests.put(url, headers=headers, json=data)
            if response.status_code == 200:
                logger.info("Successfully updated %s", config.USERS_FILE)
                return True

            logger.error(
                "Failed to update %s on attempt %d: Status %d, Response: %s",
                config.USERS_FILE,
                attempt + 1,
                response.status_code,
                response.text,
            )

        except Exception as e:
            logger.error(
                "Error updating %s on attempt %d: %s",
                config.USERS_FILE,
                attempt + 1,
                str(e),
            )

        if attempt < max_retries - 1:
            time.sleep(2)  # Wait before retrying

    return False


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /start command."""
    chat_id = update.effective_chat.id
    logger.info("Handling /start command for chat_id %s", common.mask(chat_id))

    # 1. Fetch user
    user = None
    users_data = None  # For file-based storage
    if config.USE_DATABASE:
        user = db.get_user(chat_id)
    else:
        users_data = common.read_users_file()
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)

    # 2. Check user status
    if user and user.get("pincode"):
        pincode = user.get("pincode")
        # Case A: User is already active
        if user.get("active"):
            products = user.get("products", ["Any"])

            if len(products) == 1 and products[0].lower() == "any":
                product_message = "All of the available Amul Protein products"
            else:
                display_products = [common.PRODUCT_NAME_MAP.get(p, p) for p in products]
                product_message = "\n".join(f"- {p}" for p in display_products)

            await update.message.reply_text(
                f"You have already enabled notifications for PINCODE : {pincode}.\n"
                f"You are currently tracking:\n{product_message}"
            )

        # Case B: User is inactive (reactivation)
        else:
            user["active"] = True
            if config.USE_DATABASE:
                db.update_user(chat_id, user)
            else:
                if not update_users_file(users_data):
                    await update.message.reply_text("Failed to re-enable notifications. Please try again.")
                    return

            await update.message.reply_text(
                f"Welcome back! Notifications have been re-enabled for PINCODE : {pincode}.\n"
                "Use /stop to pause them again."
            )

    # Case C: New user or user without a pincode
    else:
        await update.message.reply_text(
            "Welcome to the Amul Protein Items Notifier Bot!\n\n"
            "Use /setpincode PINCODE to set your pincode (Mandatory).\n"
            "Use /setproducts to select products (Optional, by default, we will show any Amul protein product which is available for your pincode).\n"
            "Use /stop to stop notifications."
        )

async def _save_pincode(chat_id: int, pincode: str) -> bool:
    """Helper function to save the pincode for a user, handling both DB and JSON file."""
    if config.USE_DATABASE:
        user = db.get_user(chat_id)
        if user:
            user["pincode"] = pincode
            user["active"] = True
            db.update_user(chat_id, user)
        else:
            new_user = {"chat_id": str(chat_id), "pincode": pincode, "products": ["Any"], "active": True}
            db.add_user(chat_id, new_user)
        return True
    else:
        users_data = common.read_users_file()
        users = users_data["users"]
        user = next((u for u in users if u["chat_id"] == str(chat_id)), None)
        if user:
            user["pincode"] = pincode
            user["active"] = True
        else:
            users.append({"chat_id": str(chat_id), "pincode": pincode, "products": ["Any"], "active": True})
        return update_users_file(users_data)


async def set_pincode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the conversation to set a pincode or sets it directly if provided."""
    chat_id = update.effective_chat.id
    logger.info("Handling /setpincode command for chat_id %s", common.mask(chat_id))

    if context.args:
        pincode = context.args[0]
        if not pincode.isdigit() or len(pincode) != 6:
            await update.message.reply_text("PIN code must be a 6-digit number.")
            return ConversationHandler.END

        if await _save_pincode(chat_id, pincode):
            await update.message.reply_text(f"PIN code set to {pincode}. You will receive notifications for available products.")
        else:
            await update.message.reply_text("Failed to update your PIN code. Please try again.")
        return ConversationHandler.END
    else:
        await update.message.reply_text("Please send me your 6-digit pincode.")
        return AWAITING_PINCODE


async def pincode_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the pincode received from the user during a conversation."""
    chat_id = update.effective_chat.id
    pincode = update.message.text

    if not pincode.isdigit() or len(pincode) != 6:
        await update.message.reply_text("That doesn't look like a valid 6-digit pincode. Please try again, or use /cancel to stop.")
        return AWAITING_PINCODE

    if await _save_pincode(chat_id, pincode):
        await update.message.reply_text(f"Thank you! Your PIN code has been set to {pincode}.")
    else:
        await update.message.reply_text("Failed to set your PIN code. Please try again.")
    
    return ConversationHandler.END


async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    await update.message.reply_text("Action cancelled.")
    return ConversationHandler.END


async def set_products(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /setproducts command."""
    chat_id = update.effective_chat.id
    logger.info("Handling /setproducts command for chat_id %s", common.mask(chat_id))

    if config.USE_DATABASE:
        user = db.get_user(chat_id)
    else:
        users_data = common.read_users_file()
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)

    if not user:
        await update.message.reply_text(
            "Please set your PIN code first using /setpincode PINCODE"
        )
        return

    # Initialize or retrieve the user's current selection for editing
    current_selection = set(user.get("products", ["Any"]))
    context.user_data["selected_products"] = current_selection

    keyboard = []
    for i, product in enumerate(common.PRODUCTS, 1):
        callback_data = f"product_{i}"
        display_text = common.PRODUCT_NAME_MAP[product]
        selected = "✅ " if product in current_selection else ""
        keyboard.append(
            [InlineKeyboardButton(f"{selected}{display_text}", callback_data=callback_data)]
        )

    keyboard.append(
        [InlineKeyboardButton("Confirm Selection", callback_data="confirm_products")]
    )
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Select products to monitor (click 'Any of the products from the list' for all products):\n"
        "Toggle selections, then press 'Confirm Selection'.",
        reply_markup=reply_markup,
    )


async def product_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback handler for product selection keyboard."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    logger.info("Handling product callback for chat_id %s: %s", common.mask(chat_id), query.data)

    try:
        if query.data == "confirm_products":
            selected_products = list(context.user_data.get("selected_products", []))
            if not selected_products:
                await query.message.reply_text(
                    "No products selected. Please select at least one product or 'Any of the products from the list'."
                )
                return

            if config.USE_DATABASE:
                user = db.get_user(chat_id)
                if not user:
                    await query.message.reply_text(
                        "Please set your PIN code first using /setpincode PINCODE"
                    )
                    return
                user["products"] = selected_products
                user["active"] = True
                db.update_user(chat_id, user)
                display_products = [common.PRODUCT_NAME_MAP[p] for p in selected_products]
                await query.message.reply_text(
                    f"You'll get notifications for:\n" + "\n".join(f"- {p}" for p in display_products),
                    parse_mode="Markdown",
                )
                logger.info("User %s set products: %s", common.mask(chat_id), selected_products)
                context.user_data.pop("selected_products", None)

            else:
                users_data = common.read_users_file()
                user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
                if not user:
                    await query.message.reply_text(
                        "Please set your PIN code first using /setpincode PINCODE"
                    )
                    return

                user["products"] = selected_products
                user["active"] = True

                if update_users_file(users_data):
                    display_products = [common.PRODUCT_NAME_MAP[p] for p in selected_products]
                    await query.message.reply_text(
                        f"You'll get notifications for:\n" + "\n".join(f"- {p}" for p in display_products),
                        parse_mode="Markdown",
                    )
                    logger.info("User %s set products: %s", common.mask(chat_id), selected_products)
                    context.user_data.pop("selected_products", None)
                else:
                    await query.message.reply_text("Failed to update products. Please try again.")
            return

        if query.data.startswith("product_"):
            index = int(query.data.replace("product_", "")) - 1
            if index < 0 or index >= len(common.PRODUCTS):
                logger.warning("Invalid product index %d for chat_id %s", index, common.mask(chat_id))
                return

            selected_product = common.PRODUCTS[index]
            selected_products = context.user_data.setdefault("selected_products", set())

            if selected_product == "Any":
                if "Any" in selected_products:
                    selected_products.clear()
                    logger.info("User %s deselected 'Any'", common.mask(chat_id))
                else:
                    selected_products.clear()
                    selected_products.add("Any")
                    logger.info(
                        "User %s selected 'Any', clearing specific products", common.mask(chat_id)
                    )
            else:
                selected_products.discard("Any")  # Remove 'Any' if a specific item is chosen
                if selected_product in selected_products:
                    selected_products.remove(selected_product)
                    logger.info(
                        "User %s deselected product: %s", common.mask(chat_id), selected_product
                    )
                else:
                    selected_products.add(selected_product)
                    logger.info(
                        "User %s selected product: %s", common.mask(chat_id), selected_product
                    )

            # Rebuild keyboard with updated selections
            keyboard = []
            for i, product in enumerate(common.PRODUCTS, 1):
                callback_data = f"product_{i}"
                display_text = common.PRODUCT_NAME_MAP[product]
                is_selected = product in selected_products
                selected = "✅ " if is_selected else ""
                keyboard.append(
                    [InlineKeyboardButton(f"{selected}{display_text}", callback_data=callback_data)]
                )
            keyboard.append(
                [InlineKeyboardButton("Confirm Selection", callback_data="confirm_products")]
            )
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_reply_markup(reply_markup=reply_markup)

    except Exception as e:
        logger.error("Error in product callback for chat_id %s: %s", common.mask(chat_id), str(e))
        await query.message.reply_text("An error occurred. Please try /setproducts again.")


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /stop command."""
    chat_id = update.effective_chat.id
    logger.info("Handling /stop command for chat_id %s", common.mask(chat_id))

    # Deactivate user
    if config.USE_DATABASE:
        user = db.get_user(chat_id)
        if not user or not user.get("active", False):
            await update.message.reply_text("You are not subscribed to notifications.")
            return
        user["active"] = False
        db.update_user(chat_id, user)
    else:
        users_data = common.read_users_file()
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
        if not user or not user.get("active", False):
            await update.message.reply_text("You are not subscribed to notifications.")
            return
        user["active"] = False
        if not update_users_file(users_data):
            await update.message.reply_text("Failed to stop notifications. Please try again.")
            return

    # Send confirmation with reactivation button
    keyboard = [[InlineKeyboardButton("Re-enable Notifications", callback_data="reactivate")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Notifications stopped.", reply_markup=reply_markup)


async def reactivate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback handler for the 'Re-enable Notifications' button."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id

    # Reactivation logic
    user = None
    users_data = None
    if config.USE_DATABASE:
        user = db.get_user(chat_id)
    else:
        users_data = common.read_users_file()
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)

    if user and user.get("pincode"):
        if not user.get("active"):
            user["active"] = True
            if config.USE_DATABASE:
                db.update_user(chat_id, user)
            else:
                if not update_users_file(users_data):
                    await query.edit_message_text("Failed to re-enable notifications. Please try again.")
                    return
        
        await query.edit_message_text(
            f"Welcome back! Notifications have been re-enabled for PIN code {user['pincode']}.\n"
            "Use /stop to pause them again."
        )
    else:
        await query.edit_message_text(
            "Could not find your registration. Please use /start to set up notifications."
        )


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /broadcast command."""
    chat_id = update.effective_chat.id
    if str(chat_id) != config.ADMIN_CHAT_ID:
        await update.message.reply_text("You are not authorized to use this command and your broadcast attempt has been logged.Further attempts will lead to a ban.")
        logger.warning("Unauthorized broadcast attempt by chat_id %s", common.mask(chat_id))
        return

    message_to_broadcast = ' '.join(context.args)
    if not message_to_broadcast:
        await update.message.reply_text("Please provide a message to broadcast. Usage: /broadcast <message>")
        return

    context.user_data['broadcast_message'] = message_to_broadcast

    keyboard = [
        [InlineKeyboardButton("Accept", callback_data='broadcast_accept')],
        [InlineKeyboardButton("Reject", callback_data='broadcast_reject')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"You are about to send the following message to all active users:\n\n---\n{message_to_broadcast}\n---\n\nPlease confirm.",
        reply_markup=reply_markup
    )


async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback handler for broadcast confirmation."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    
    if str(chat_id) != config.ADMIN_CHAT_ID:
        logger.warning("Unauthorized broadcast callback interaction by chat_id %s", common.mask(chat_id))
        return

    if query.data == 'broadcast_accept':
        message = context.user_data.get('broadcast_message')
        if not message:
            await query.edit_message_text("Error: Broadcast message not found. Please try again.")
            return

        if config.USE_DATABASE:
            all_users = db.get_all_users()
            active_users = [user for user in all_users if user.get('active')]
        else:
            users_data = common.read_users_file()
            all_users = users_data.get("users", [])
            active_users = [user for user in all_users if user.get('active')]
        
        sent_count = 0
        for user in active_users:
            try:
                await context.bot.send_message(chat_id=user['chat_id'], text=message)
                sent_count += 1
                await asyncio.sleep(0.1) # Small delay to avoid hitting rate limits
            except Exception as e:
                logger.error("Failed to send broadcast to chat_id %s: %s", common.mask(user['chat_id']), e)

        await query.edit_message_text(f"Broadcast sent to {sent_count} active users.")
        logger.info("Admin %s sent broadcast to %d users.", common.mask(chat_id), sent_count)
        context.user_data.pop('broadcast_message', None)

    elif query.data == 'broadcast_reject':
        await query.edit_message_text("Broadcast canceled.")
        logger.info("Admin %s canceled broadcast.", common.mask(chat_id))
        context.user_data.pop('broadcast_message', None)

async def run_polling(app: Application):
    """Starts the bot in polling mode."""
    await app.initialize()
    await app.start()
    await app.updater.start_polling(timeout=5)
    logger.info("Polling started")
    try:
        await asyncio.Event().wait()  # Keep running until interrupted
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Polling stopped")
    finally:
        logger.info("Shutting down bot...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        if config.USE_DATABASE:
            db.close()
        logger.info("Bot shutdown complete")

def main():
    """Main entry point for the bot."""
    logger.info("Starting main function")
    if common.is_already_running("main.py"):
        logger.error("Another instance of the bot is already running. Exiting...")
        raise SystemExit(1)

    app = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()

    # Setup conversation handler for setpincode
    pincode_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("setpincode", set_pincode)],
        states={
            AWAITING_PINCODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, pincode_received)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    # Register handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(pincode_conv_handler)
    app.add_handler(CommandHandler("setproducts", set_products))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern='^broadcast_'))
    app.add_handler(CallbackQueryHandler(reactivate_callback, pattern='^reactivate$'))
    app.add_handler(CallbackQueryHandler(product_callback))

    asyncio.run(run_polling(app))


if __name__ == "__main__":
    main()

