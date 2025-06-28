import asyncio
from urllib.parse import quote
import aiohttp
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.constants import ParseMode
from dotenv import load_dotenv
import os
from database import DatabaseManager

load_dotenv()

# Initialize logger and database
from logger import create_logger
logger = create_logger(
    name="NyaaBot",
    telegram_token=os.getenv("LOG_BOT_TOKEN"),
    telegram_channel=os.getenv("LOG_CHANNEL_ID")
)
db = DatabaseManager()

# Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN") 
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

class NyaaBot:
    def __init__(self):
        self.session = None
    
    async def init_session(self):
        """Initialize aiohttp session"""
        if not self.session:
            self.session = aiohttp.ClientSession(headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30))
    
    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
    
    async def search_nyaa(self, query: str, page: int = 1):
        """Search nyaa.si and return results"""
        await self.init_session()
        
        search_url = f"https://nyaa.si/?f=0&c=0_0&q={quote(query)}&p={page}"
        
        try:
            async with self.session.get(search_url) as response:
                if response.status != 200:
                    return None
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                results = []
                for row in soup.select("table.torrent-list tbody tr"):
                    try:
                        # Get title and link
                        link_tag = row.find("a", href=lambda x: x and x.startswith("/view/"))
                        if not link_tag:
                            continue
                        
                        title = link_tag.get_text(strip=True)
                        view_url = "https://nyaa.si" + link_tag['href']
                        
                        # Get size
                        size_cell = row.select_one("td:nth-of-type(4)")
                        size = size_cell.get_text(strip=True) if size_cell else "Unknown"
                        
                        # Get seeders and leechers
                        seeders_cell = row.select_one("td:nth-of-type(6)")
                        leechers_cell = row.select_one("td:nth-of-type(7)")
                        
                        seeders = seeders_cell.get_text(strip=True) if seeders_cell else "0"
                        leechers = leechers_cell.get_text(strip=True) if leechers_cell else "0"
                        
                        results.append({
                            'title': title,
                            'url': view_url,
                            'size': size,
                            'seeders': seeders,
                            'leechers': leechers
                        })
                    except Exception as e:
                        logger.error(f"Error parsing row: {e}")
                        continue
                
                return results
                
        except Exception as e:
            logger.error(f"Search error: {e}")
            return None
    
    async def get_magnet_link(self, url: str):
        """Extract magnet link from result page"""
        await self.init_session()
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return None
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                magnet_tag = soup.find('a', href=lambda x: x and x.startswith('magnet:'))
                return magnet_tag['href'] if magnet_tag else None
                
        except Exception as e:
            logger.error(f"Magnet extraction error: {e}")
            return None

# Initialize bot instance
nyaa_bot = NyaaBot()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    
    # Create/update user in database
    await db.create_or_update_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )
    
    logger.log_user_action("START_COMMAND", user.id, user.username)
    
    welcome_text = """
🔍 **Nyaa.si Search Bot**

Welcome! I can help you search for torrents on nyaa.si

📝 **Commands:**
• Send any message to search
• /help - Show this help message

🚀 **How to use:**
Just type what you're looking for and I'll search nyaa.si for you!
    """
    
    keyboard = [
        [InlineKeyboardButton("🔍 Start Searching", callback_data="start_search")],
        [InlineKeyboardButton("📊 My Stats", callback_data="user_stats")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        welcome_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    await start(update, context)

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stats command"""
    user = update.effective_user
    
    logger.log_user_action("STATS_COMMAND", user.id, user.username)
    
    stats = await db.get_user_stats(user.id)
    
    if not stats:
        await update.message.reply_text("❌ No statistics available. Start by searching something!")
        return
    
    stats_text = f"""
📊 **Your Statistics**

🔍 **Total Searches:** {stats.get('total_searches', 0)}
📥 **Total Downloads:** {stats.get('total_downloads', 0)}
📅 **Today's Searches:** {stats.get('today_searches', 0)}
📅 **Today's Downloads:** {stats.get('today_downloads', 0)}

👤 **Member Since:** {stats.get('member_since', 'Unknown').strftime('%Y-%m-%d') if stats.get('member_since') else 'Unknown'}
🕐 **Last Active:** {stats.get('last_seen', 'Unknown').strftime('%Y-%m-%d %H:%M') if stats.get('last_seen') else 'Unknown'}
    """
    
    keyboard = [[InlineKeyboardButton("🔍 Start Searching", callback_data="start_search")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        stats_text.strip(),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /history command"""
    user = update.effective_user
    
    logger.log_user_action("HISTORY_COMMAND", user.id, user.username)
    
    history = await db.get_user_search_history(user.id, limit=10)
    
    if not history:
        await update.message.reply_text("📝 No search history found. Start searching to build your history!")
        return
    
    history_text = "📝 **Your Recent Searches:**\n\n"
    
    for i, search in enumerate(history, 1):
        date = search['created_at'].strftime('%m-%d %H:%M')
        query = search['query'][:30] + "..." if len(search['query']) > 30 else search['query']
        results = search.get('results_count', 0)
        
        history_text += f"**{i}.** `{query}`\n"
        history_text += f"   📅 {date} | 📊 {results} results\n\n"
    
    keyboard = [[InlineKeyboardButton("🔍 Start Searching", callback_data="start_search")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        history_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /admin_stats command (for bot administrators)"""
    user = update.effective_user
    
    # Add your admin user IDs here
    ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ You don't have permission to use this command.")
        return
    
    logger.log_user_action("ADMIN_STATS", user.id, user.username)
    
    try:
        bot_stats = await db.get_bot_stats()
        popular_searches = await db.get_popular_searches(limit=5)
        
        stats_text = f"""
🤖 **Bot Statistics**

👥 **Users:**
• Total Users: {bot_stats['total_users']}
• Active Users (7d): {bot_stats['active_users_7d']}

🔍 **Activity:**
• Total Searches: {bot_stats['total_searches']}
• Total Downloads: {bot_stats['total_downloads']}
• Today's Searches: {bot_stats['today_searches']}
• Today's Downloads: {bot_stats['today_downloads']}

🔥 **Popular Searches (7 days):**
"""
        
        for i, search in enumerate(popular_searches, 1):
            query = search['query'][:25] + "..." if len(search['query']) > 25 else search['query']
            stats_text += f"{i}. `{query}` ({search['count']} searches)\n"
        
        await update.message.reply_text(
            stats_text,
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        logger.error(f"Error getting admin stats: {e}", user_id=user.id, username=user.username)
        await update.message.reply_text("❌ Error retrieving statistics.")

async def search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle search queries"""
    user = update.effective_user
    query = update.message.text.strip()
    
    if not query:
        await update.message.reply_text("❌ Please provide a search query!")
        return
    
    # Update user info
    await db.create_or_update_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )
    
    logger.log_user_action("SEARCH", user.id, user.username, f"query: {query}")
    
    # Show searching message
    search_msg = await update.message.reply_text("🔍 Searching nyaa.si...")
    
    # Perform search
    results = await nyaa_bot.search_nyaa(query)
    
    if not results:
        await search_msg.edit_text("❌ No results found or search failed. Please try again.")
        # Save search with 0 results
        await db.save_search(user.id, query, 0)
        return
    
    # Save search to database
    await db.save_search(user.id, query, len(results))
    
    # Store search results in context for pagination
    context.user_data['search_results'] = results
    context.user_data['search_query'] = query
    context.user_data['current_page'] = 0
    
    # Show results
    await show_results_page(search_msg, context, 0)

async def show_results_page(message, context: ContextTypes.DEFAULT_TYPE, page: int):
    """Display a page of search results"""
    results = context.user_data.get('search_results', [])
    query = context.user_data.get('search_query', '')
    
    if not results:
        await message.edit_text("❌ No results to display.")
        return
    
    # Pagination settings
    per_page = 5
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, len(results))
    page_results = results[start_idx:end_idx]
    
    # Build results text
    results_text = f"🔍 **Search Results for:** `{query}`\n"
    results_text += f"📊 **Page {page + 1}** ({start_idx + 1}-{end_idx} of {len(results)})\n\n"
    
    # Create inline keyboard for results
    keyboard = []
    
    for i, result in enumerate(page_results, start=start_idx):
        title = result['title'][:50] + "..." if len(result['title']) > 50 else result['title']
        
        results_text += f"**{i + 1}.** {result['title']}\n"
        results_text += f"   📦 Size: `{result['size']}` | 🌱 S: `{result['seeders']}` | 📥 L: `{result['leechers']}`\n\n"
        
        keyboard.append([InlineKeyboardButton(
            f"📥 #{i + 1} Magnet Link", 
            callback_data=f"get_magnet:{i}"
        )])
    
    # Navigation buttons
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"page:{page - 1}"))
    
    if end_idx < len(results):
        nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f"page:{page + 1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Add new search button
    keyboard.append([InlineKeyboardButton("🔍 New Search", callback_data="start_search")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await message.edit_text(
        results_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline keyboard button presses"""
    query = update.callback_query
    user = update.effective_user
    await query.answer()
    
    data = query.data
    
    if data == "start_search":
        await query.edit_message_text(
            "🔍 **Ready to search!**\n\nJust send me what you want to search for on nyaa.si",
            parse_mode=ParseMode.MARKDOWN
        )
    
    elif data == "user_stats":
        logger.log_user_action("STATS_BUTTON", user.id, user.username)
        
        stats = await db.get_user_stats(user.id)
        
        if not stats:
            await query.edit_message_text("❌ No statistics available. Start by searching something!")
            return
        
        stats_text = f"""
📊 **Your Statistics**

🔍 **Total Searches:** {stats.get('total_searches', 0)}
📥 **Total Downloads:** {stats.get('total_downloads', 0)}
📅 **Today's Activity:** {stats.get('today_searches', 0)} searches, {stats.get('today_downloads', 0)} downloads

👤 **Member Since:** {stats.get('member_since').strftime('%Y-%m-%d') if stats.get('member_since') else 'Unknown'}
        """
        
        keyboard = [[InlineKeyboardButton("🔍 Start Searching", callback_data="start_search")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            stats_text.strip(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    
    elif data.startswith("page:"):
        page = int(data.split(":")[1])
        context.user_data['current_page'] = page
        await show_results_page(query.message, context, page)
    
    elif data.startswith("get_magnet:"):
        result_idx = int(data.split(":")[1])
        await get_magnet_handler(query, context, result_idx)

async def get_magnet_handler(query, context: ContextTypes.DEFAULT_TYPE, result_idx: int):
    """Handle magnet link extraction"""
    user = query.from_user
    results = context.user_data.get('search_results', [])
    
    if result_idx >= len(results):
        await query.edit_message_text("❌ Invalid selection!")
        return
    
    result = results[result_idx]
    
    logger.log_user_action("DOWNLOAD_REQUEST", user.id, user.username, f"title: {result['title'][:50]}")
    
    # Show loading message
    loading_msg = await query.edit_message_text(
        f"🔄 **Getting magnet link for:**\n`{result['title']}`\n\nPlease wait...",
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Get magnet link
    magnet_link = await nyaa_bot.get_magnet_link(result['url'])
    
    if not magnet_link:
        keyboard = [[InlineKeyboardButton("🔙 Back to Results", callback_data=f"page:{context.user_data.get('current_page', 0)}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await loading_msg.edit_text(
            "❌ **Failed to get magnet link!**\n\nThe torrent page might be unavailable.",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Save download to database
    await db.save_download(
        user_id=user.id,
        title=result['title'],
        magnet_link=magnet_link,
        size=result['size'],
        seeders=result['seeders']
    )
    
    logger.log_user_action("DOWNLOAD_SUCCESS", user.id, user.username, f"title: {result['title'][:50]}")
    
    # Format result with magnet link
    result_text = f"✅ **Magnet Link Retrieved!**\n\n"
    result_text += f"**Title:** `{result['title']}`\n"
    result_text += f"**Size:** `{result['size']}`\n"
    result_text += f"**Seeders:** `{result['seeders']}` | **Leechers:** `{result['leechers']}`\n\n"
    result_text += f"**📋 Magnet Link (Tap to Copy):**\n"
    result_text += f"`{magnet_link}`\n\n"
    result_text += "💡 **How to use:**\n"
    result_text += "1. Tap and hold the magnet link above\n"
    result_text += "2. Select 'Copy' from the menu\n"
    result_text += "3. Paste it into your torrent client\n"
    result_text += "4. Or tap the link to open directly if you have a torrent app installed"
    
    # Create buttons
    keyboard = [
        [InlineKeyboardButton("🔙 Back to Results", callback_data=f"page:{context.user_data.get('current_page', 0)}")],
        [InlineKeyboardButton("🔍 New Search", callback_data="start_search")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await loading_msg.edit_text(
        result_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    user = update.effective_user if update.effective_user else None
    user_id = user.id if user else None
    username = user.username if user else None
    
    logger.error(f"Update {update} caused error {context.error}", 
                user_id=user_id, username=username, exc_info=context.error)
    
    if update.effective_message:
        await update.effective_message.reply_text(
            "❌ **Oops! Something went wrong.**\n\nPlease try again or contact the administrator if the problem persists.",
            parse_mode=ParseMode.MARKDOWN
        )

async def init_database():
    """Initialize database connection"""
    try:
        await db.connect()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

async def cleanup():
    """Cleanup resources"""
    await nyaa_bot.close_session()
    await db.disconnect()
    await logger.cleanup()  # Cleanup logger resources
    logger.log_bot_stop()
    logger.info("Cleanup completed")

def main():
    """Start the bot"""
    print("🤖 Starting Enhanced Nyaa.si Telegram Bot...")
    
    # Create application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("history", history_command))
    application.add_handler(CommandHandler("admin_stats", admin_stats_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_handler))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Initialize database on startup
    application.job_queue.run_once(lambda _: asyncio.create_task(init_database()), when=0)
    
    # Schedule cleanup on shutdown (this won't work perfectly, but it's better than nothing)
    application.job_queue.run_once(lambda _: asyncio.create_task(cleanup()), when=3600)  # Run after 1 hour as fallback
    
    logger.log_bot_start()
    logger.info("Bot is ready! Send /start to begin.")
    print("✅ Bot is ready! Send /start to begin.")
    
    # Start bot
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        print("🛑 Bot stopped by user")
        # Run cleanup on keyboard interrupt
        asyncio.run(cleanup())
    except Exception as e:
        logger.error(f"Bot crashed: {e}", exc_info=e)
        print(f"💥 Bot crashed: {e}")
        # Run cleanup on crash
        asyncio.run(cleanup())

if __name__ == "__main__":
    main()