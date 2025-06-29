import asyncio
from urllib.parse import quote
import aiohttp
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.constants import ParseMode
from dotenv import load_dotenv
import os

load_dotenv()

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
                        print(f"Error parsing row: {e}")
                        continue
                
                return results
                
        except Exception as e:
            print(f"Search error: {e}")
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
            print(f"Magnet extraction error: {e}")
            return None

# Initialize bot instance
nyaa_bot = NyaaBot()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user
    
    print(f"User {user.username} ({user.id}) started the bot")
    
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
        [InlineKeyboardButton("🔍 Start Searching", callback_data="start_search")]
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

async def search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle search queries"""
    user = update.effective_user
    query = update.message.text.strip()
    
    if not query:
        await update.message.reply_text("❌ Please provide a search query!")
        return
    
    print(f"User {user.username} ({user.id}) searched for: {query}")
    
    # Show searching message
    search_msg = await update.message.reply_text("🔍 Searching nyaa.si...")
    
    # Perform search
    results = await nyaa_bot.search_nyaa(query)
    
    if not results:
        await search_msg.edit_text("❌ No results found or search failed. Please try again.")
        return
    
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
    
    print(f"User {user.username} ({user.id}) requested magnet for: {result['title'][:50]}")
    
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
    
    print(f"Successfully retrieved magnet link for user {user.username} ({user.id})")
    
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
    
    print(f"Error for user {username} ({user_id}): {context.error}")
    
    if update.effective_message:
        await update.effective_message.reply_text(
            "❌ **Oops! Something went wrong.**\n\nPlease try again or contact the administrator if the problem persists.",
            parse_mode=ParseMode.MARKDOWN
        )

async def cleanup():
    """Cleanup resources"""
    await nyaa_bot.close_session()
    print("Cleanup completed")

def main():
    """Start the bot"""
    print("🤖 Starting Nyaa.si Telegram Bot...")
    
    # Create application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, search_handler))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    print("✅ Bot is ready! Send /start to begin.")
    
    # Start bot
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except KeyboardInterrupt:
        print("🛑 Bot stopped by user")
        # Run cleanup on keyboard interrupt
        asyncio.run(cleanup())
    except Exception as e:
        print(f"💥 Bot crashed: {e}")
        # Run cleanup on crash
        asyncio.run(cleanup())

if __name__ == "__main__":
    main()