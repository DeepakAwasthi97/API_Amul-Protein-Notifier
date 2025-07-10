import asyncio
import json
import base64
import aiohttp
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

async def update_users_file(users_data):
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

    # 1. Fetch user
    user = None
    users_data = None
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
                f"You have already enabled notifications for PIN code {pincode}.\n"
                f"You are currently tracking:\n{product_message}"
            )

        # Case B: User is inactive (reactivation)
        else:
            user["active"] = True
            if config.USE_DATABASE:
                db.update_user(chat_id, user)
            else:
                if not await update_users_file(users_data):
                    await update.message.reply_text("Failed to re-enable notifications. Please try again.")
                    return

            await update.message.reply_text(
                f"Welcome back! Notifications have been re-enabled for PIN code {pincode}.\n"
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
        return await update_users_file(users_data)

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

async def set_products(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Starts the product selection conversation."""
    chat_id = update.effective_chat.id
    logger.info("Starting product selection for chat_id %s", common.mask(chat_id))

    if config.USE_DATABASE:
        user = db.get_user(chat_id)
    else:
        users_data = common.read_users_file()
        user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)

    context.user_data["selected_products"] = set()
    context.user_data["product_menu_view"] = "main"
    
    keyboard = [
        [InlineKeyboardButton("Browse by Category", callback_data="products_nav_cat_list")],
        [InlineKeyboardButton("List All Products", callback_data="products_nav_all")],
        [InlineKeyboardButton("Track Any Available Product", callback_data="products_confirm_Any")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Please select the products you want to monitor.", reply_markup=reply_markup
    )

async def set_products_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all interactions for the product selection menu."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id
    selected_products = context.user_data.get("selected_products", set())
    action = query.data
    action_for_rendering = action

    try:
        # Handle confirmation before navigating back
        if action == "products_nav_main" and selected_products:
            display_products = [common.PRODUCT_NAME_MAP.get(p, p) for p in selected_products]
            product_list_text = "\n".join(f"- {p}" for p in display_products)
            confirmation_text = (
                "Please confirm your selection of the products to be given notifications for "
                "or select clear selections and go back to main menu:\n\n" +
                f"{product_list_text}"
            )
            confirmation_keyboard = [
                [InlineKeyboardButton("Confirm Selection", callback_data="products_confirm_and_back")],
                [InlineKeyboardButton("Clear Selection & Back to Main Menu", callback_data="products_clear_and_back_to_main")],
            ]
            reply_markup = InlineKeyboardMarkup(confirmation_keyboard)
            await query.edit_message_text(text=confirmation_text, reply_markup=reply_markup)
            return

        # Handle product toggle
        if action.startswith("products_toggle_"):
            product_index = int(action.replace("products_toggle_", ""))
            if product_index < len(common.PRODUCTS):
                product_name = common.PRODUCTS[product_index]
                if product_name in selected_products:
                    selected_products.remove(product_name)
                else:
                    selected_products.add(product_name)
            action_for_rendering = f"products_view_cat_{context.user_data.get('product_menu_category', '')}" if context.user_data.get("product_menu_view") == "category" else "products_nav_all"

        # Handle clear selection
        elif action == "products_clear":
            current_category = context.user_data.get("product_menu_category")
            if current_category:
                products_to_clear = set(common.CATEGORIZED_PRODUCTS[current_category])
                selected_products -= products_to_clear
            else:
                selected_products.clear()
            action_for_rendering = f"products_view_cat_{current_category}" if current_category else "products_nav_all"

        # Final actions
        elif action == "products_confirm_Any":
            final_selection = ["Any"]
            if config.USE_DATABASE:
                user = db.get_user(chat_id)
                user["products"] = final_selection
                user["active"] = True
                try:
                    db.update_user(chat_id, user)
                    await query.edit_message_text("✅ Your selection has been saved. You will now be notified if **Any** of the Amul Protein product is available.")
                except Exception as e:
                    logger.error("Database error for chat_id %s: %s", common.mask(chat_id), str(e))
                    await query.edit_message_text("Failed to save your selection. Please try again later.")
            else:
                users_data = common.read_users_file()
                user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
                user["products"] = final_selection
                user["active"] = True
                if await update_users_file(users_data):
                    await query.edit_message_text("✅ Your selection has been saved. You will now be notified if **Any** of the Amul Protein product is available.")
                else:
                    await query.edit_message_text("Failed to save your selection. Please try again later.")
            for key in [k for k in context.user_data if k.startswith("product_menu_")]:
                del context.user_data[key]
            context.user_data["selected_products"] = set()
            return

        elif action == "products_confirm":
            if not selected_products:
                if config.USE_DATABASE:
                    user = db.get_user(chat_id)
                else:
                    users_data = common.read_users_file()
                    user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)

                current_tracked_products = user.get("products", ["Any"])
                if len(current_tracked_products) == 1 and current_tracked_products[0].lower() == "any":
                    product_message = "All of the available Amul Protein products"
                else:
                    display_products = [common.PRODUCT_NAME_MAP.get(p, p) for p in current_tracked_products]
                    product_message = "\n".join(f"- {p}" for p in display_products)

                await query.edit_message_text(
                    f"No products were selected. You are currently tracking:\n{product_message}"
                )
                for key in [k for k in context.user_data if k.startswith("product_menu_")]:
                    del context.user_data[key]
                context.user_data["selected_products"] = set()
                return

            final_selection = ["Any"] if "Any" in selected_products else list(selected_products)
            if config.USE_DATABASE:
                user = db.get_user(chat_id)
                user["products"] = final_selection
                user["active"] = True
                try:
                    db.update_user(chat_id, user)
                    product_message = "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in final_selection)
                    await query.edit_message_text(f"Your selections have been saved. You will be notified for:\n{product_message}")
                except Exception as e:
                    logger.error("Database error for chat_id %s: %s", common.mask(chat_id), str(e))
                    await query.edit_message_text("Failed to save your selections. Please try again later.")
            else:
                users_data = common.read_users_file()
                user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
                user["products"] = final_selection
                user["active"] = True
                if await update_users_file(users_data):
                    product_message = "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in final_selection)
                    await query.edit_message_text(f"Your selections have been saved. You will be notified for:\n{product_message}")
                else:
                    await query.edit_message_text("Failed to save your selections. Please try again later.")
            for key in [k for k in context.user_data if k.startswith("product_menu_")]:
                del context.user_data[key]
            context.user_data["selected_products"] = set()
            return

        elif action == "products_confirm_and_back":
            final_selection = ["Any"] if "Any" in selected_products else list(selected_products)
            if config.USE_DATABASE:
                user = db.get_user(chat_id)
                user["products"] = final_selection
                user["active"] = True
                try:
                    db.update_user(chat_id, user)
                    product_message = "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in final_selection)
                    await query.edit_message_text(f"Your selections have been saved. You will be notified for:\n{product_message}")
                except Exception as e:
                    logger.error("Database error for chat_id %s: %s", common.mask(chat_id), str(e))
                    await query.edit_message_text("Failed to save your selections. Please try again later.")
            else:
                users_data = common.read_users_file()
                user = next((u for u in users_data["users"] if u["chat_id"] == str(chat_id)), None)
                user["products"] = final_selection
                user["active"] = True
                if await update_users_file(users_data):
                    product_message = "\n".join(f"- {common.PRODUCT_NAME_MAP.get(p, p)}" for p in final_selection)
                    await query.edit_message_text(f"Your selections have been saved. You will be notified for:\n{product_message}")
                else:
                    await query.edit_message_text("Failed to save your selections. Please try again later.")
            for key in [k for k in context.user_data if k.startswith("product_menu_")]:
                del context.user_data[key]
            context.user_data["selected_products"] = set()
            return

        elif action == "products_clear_and_back_to_main":
            selected_products.clear()
            for key in [k for k in context.user_data if k.startswith("product_menu_")]:
                del context.user_data[key]
            context.user_data["selected_products"] = set()
            context.user_data["product_menu_view"] = "main"
            action_for_rendering = "products_nav_main"

        # Menu rendering
        keyboard = []
        text = ""
        if action_for_rendering == "products_nav_main":
            context.user_data["product_menu_view"] = "main"
            text = "Please select the products you want to monitor."
            keyboard.extend([
                [InlineKeyboardButton("Browse by Category", callback_data="products_nav_cat_list")],
                [InlineKeyboardButton("List All Products", callback_data="products_nav_all")],
                [InlineKeyboardButton("Track Any Available Product", callback_data="products_confirm_Any")],
            ])
        elif action_for_rendering == "products_nav_cat_list":
            context.user_data["product_menu_view"] = "cat_list"
            text = "Select a category to view products."
            for category in common.CATEGORIES:
                keyboard.append([InlineKeyboardButton(category, callback_data=f"products_view_cat_{category}")])
        elif action_for_rendering.startswith("products_view_cat_"):
            category = action_for_rendering.replace("products_view_cat_", "")
            context.user_data["product_menu_view"] = "category"
            context.user_data["product_menu_category"] = category
            text = f"Products in {category}:"
            for product_name in common.CATEGORIZED_PRODUCTS[category]:
                product_index = common.PRODUCTS.index(product_name)
                selected_marker = "✅ " if product_name in selected_products else ""
                keyboard.append([InlineKeyboardButton(f"{selected_marker}{common.PRODUCT_NAME_MAP.get(product_name, product_name)}", callback_data=f"products_toggle_{product_index}")])
        elif action_for_rendering == "products_nav_all":
            context.user_data["product_menu_view"] = "all"
            text = "Select products to monitor:"
            for i, product_name in enumerate(common.PRODUCTS):
                if product_name == "Any": continue
                selected_marker = "✅ " if product_name in selected_products else ""
                keyboard.append([InlineKeyboardButton(f"{selected_marker}{common.PRODUCT_NAME_MAP.get(product_name, product_name)}", callback_data=f"products_toggle_{i}")])

        if action_for_rendering not in ["products_nav_main", "products_nav_cat_list"]:
            keyboard.append([
                InlineKeyboardButton("Confirm Selection", callback_data="products_confirm"),
                InlineKeyboardButton("Clear Selection", callback_data="products_clear"),
            ])

        if context.user_data.get("product_menu_view") == "cat_list":
            keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="products_nav_main")])
        elif context.user_data.get("product_menu_view") == "category":
            keyboard.append([InlineKeyboardButton("⬅️ Back to Categories", callback_data="products_nav_cat_list")])
        elif context.user_data.get("product_menu_view") == "all":
            keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="products_nav_main")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text=text, reply_markup=reply_markup)

    except Exception as e:
        logger.error("Error in set_products_callback for chat_id %s: %s", common.mask(chat_id), str(e))
        await query.edit_message_text("An error occurred. Please try again or use /setproducts to restart.")
        return

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /stop command."""
    chat_id = update.effective_chat.id
    logger.info("Handling /stop command for chat_id %s", common.mask(chat_id))

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
        if not await update_users_file(users_data):
            await update.message.reply_text("Failed to stop notifications. Please try again.")
            return

    keyboard = [[InlineKeyboardButton("Re-enable Notifications", callback_data="reactivate")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Notifications stopped.", reply_markup=reply_markup)

async def reactivate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback handler for the 'Re-enable Notifications' button."""
    query = update.callback_query
    await query.answer()
    chat_id = query.from_user.id

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
                if not await update_users_file(users_data):
                    await query.edit_message_text("Failed to re-enable notifications. Please try again.")
                    return
        
        await query.edit_message_text(f"Welcome back! Notifications have been re-enabled for PIN code {user['pincode']}.\nUse /stop to pause them again.")
    else:
        await query.edit_message_text("Could not find your registration. Please use /start to set up notifications.")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /broadcast command."""
    chat_id = update.effective_chat.id
    if str(chat_id) != config.ADMIN_CHAT_ID:
        await update.message.reply_text("You are not authorized to use this command.")
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
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error("Failed to send broadcast to chat_id %s: %s", common.mask(user['chat_id']), e)

        await query.edit_message_text(f"Broadcast sent to {sent_count} active users.")
        logger.info("Admin %s sent broadcast to %d users.", common.mask(chat_id), sent_count)
        context.user_data.pop('broadcast_message', None)

    elif query.data == 'broadcast_reject':
        await query.edit_message_text("Broadcast canceled.")
        logger.info("Admin %s canceled broadcast.", common.mask(chat_id))
        context.user_data.pop("broadcast_message", None)

async def run_polling(app: Application):
    """Starts the bot in polling mode."""
    await app.initialize()
    await app.start()
    await app.updater.start_polling(timeout=5)
    logger.info("Polling started")
    try:
        await asyncio.Event().wait()
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

    pincode_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("setpincode", set_pincode)],
        states={
            AWAITING_PINCODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, pincode_received)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(pincode_conv_handler)
    app.add_handler(CommandHandler("setproducts", set_products))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CallbackQueryHandler(set_products_callback, pattern='^products_'))
    app.add_handler(CallbackQueryHandler(reactivate_callback, pattern='^reactivate$'))
    app.add_handler(CallbackQueryHandler(broadcast_callback, pattern='^broadcast_'))

    asyncio.run(run_polling(app))

if __name__ == "__main__":
    main()