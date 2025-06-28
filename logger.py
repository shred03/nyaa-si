# logger.py
import logging
import os
import asyncio
from datetime import datetime
from pathlib import Path
import aiohttp
from telegram import Bot
from telegram.constants import ParseMode


class TelegramHandler(logging.Handler):
    """Custom logging handler that sends logs to a Telegram channel"""
    
    def __init__(self, bot_token: str, channel_id: str, level: int = logging.ERROR):
        super().__init__(level)
        self.bot_token = bot_token
        self.channel_id = channel_id
        self.bot = Bot(token=bot_token)
        self.session = None
        
        # Rate limiting
        self.last_sent = {}
        self.rate_limit_seconds = 5
        
    async def init_session(self):
        """Initialize aiohttp session"""
        if not self.session:
            self.session = aiohttp.ClientSession()
    
    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
    
    def emit(self, record):
        """Send log record to Telegram channel"""
        try:
            # Create event loop if it doesn't exist
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Run the async send method
            if loop.is_running():
                # If loop is already running, schedule the task
                asyncio.create_task(self._send_to_telegram(record))
            else:
                # If loop is not running, run it
                loop.run_until_complete(self._send_to_telegram(record))
        except Exception as e:
            # Fallback to standard error handling
            self.handleError(record)
    
    async def _send_to_telegram(self, record):
        """Async method to send log to Telegram"""
        try:
            message = self.format(record)
            
            # Rate limiting check
            message_hash = hash(message)
            current_time = datetime.now().timestamp()
            
            if (message_hash in self.last_sent and 
                current_time - self.last_sent[message_hash] < self.rate_limit_seconds):
                return
            
            self.last_sent[message_hash] = current_time
            
            # Format message for Telegram
            telegram_message = self._format_for_telegram(record, message)
            
            # Send to Telegram
            await self.bot.send_message(
                chat_id=self.channel_id,
                text=telegram_message,
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            # Don't let Telegram errors break the application
            print(f"Failed to send log to Telegram: {e}")
    
    def _format_for_telegram(self, record, message):
        """Format log message for Telegram"""
        level_emoji = {
            'DEBUG': '🐛',
            'INFO': 'ℹ️',
            'WARNING': '⚠️',
            'ERROR': '❌',
            'CRITICAL': '🚨'
        }
        
        emoji = level_emoji.get(record.levelname, '📝')
        
        # Escape markdown special characters
        message = message.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace(']', '\\]')
        
        telegram_message = f"{emoji} **{record.levelname}**\n"
        telegram_message += f"⏰ `{datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')}`\n"
        telegram_message += f"📍 `{record.name}`\n\n"
        telegram_message += f"```\n{message}\n```"
        
        # Add exception info if available
        if record.exc_info:
            telegram_message += f"\n\n**Stack Trace:**\n```\n{self.format_exception(record.exc_info)}\n```"
        
        # Telegram message limit is 4096 characters
        if len(telegram_message) > 4000:
            telegram_message = telegram_message[:3900] + "\n\n... (truncated)"
        
        return telegram_message
    
    def format_exception(self, ei):
        """Format exception info"""
        import traceback
        return ''.join(traceback.format_exception(*ei))

class BotLogger:
    def __init__(self, name: str = "NyaaBot", log_dir: str = "logs", 
                 telegram_token: str = None, telegram_channel: str = None):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers(telegram_token, telegram_channel)
    
    def _setup_handlers(self, telegram_token: str = None, telegram_channel: str = None):
        """Setup file, console, and telegram handlers"""
        # File handler with daily rotation
        today = datetime.now().strftime("%Y-%m-%d")
        log_file = self.log_dir / f"{self.name}_{today}.log"
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Telegram handler (only for errors and critical by default)
        if telegram_token and telegram_channel:
            try:
                telegram_handler = TelegramHandler(
                    bot_token=telegram_token,
                    channel_id=telegram_channel,
                    level=logging.ERROR
                )
                telegram_handler.setFormatter(formatter)
                self.logger.addHandler(telegram_handler)
                print(f"✅ Telegram logging enabled for channel: {telegram_channel}")
            except Exception as e:
                print(f"⚠️ Failed to setup Telegram logging: {e}")
    
    def info(self, message: str, user_id: int = None, username: str = None):
        """Log info message with optional user context"""
        context = self._build_context(user_id, username)
        self.logger.info(f"{context}{message}")
    
    def error(self, message: str, user_id: int = None, username: str = None, exc_info=None):
        """Log error message with optional user context"""
        context = self._build_context(user_id, username)
        self.logger.error(f"{context}{message}", exc_info=exc_info)
    
    def warning(self, message: str, user_id: int = None, username: str = None):
        """Log warning message with optional user context"""
        context = self._build_context(user_id, username)
        self.logger.warning(f"{context}{message}")
    
    def debug(self, message: str, user_id: int = None, username: str = None):
        """Log debug message with optional user context"""
        context = self._build_context(user_id, username)
        self.logger.debug(f"{context}{message}")
    
    def critical(self, message: str, user_id: int = None, username: str = None):
        """Log critical message with optional user context"""
        context = self._build_context(user_id, username)
        self.logger.critical(f"{context}{message}")
    
    def _build_context(self, user_id: int = None, username: str = None) -> str:
        """Build user context string for logging"""
        if user_id or username:
            context_parts = []
            if user_id:
                context_parts.append(f"user_id:{user_id}")
            if username:
                context_parts.append(f"username:{username}")
            return f"[{', '.join(context_parts)}] "
        return ""
    
    def log_user_action(self, action: str, user_id: int, username: str = None, details: str = None):
        """Log user actions for analytics"""
        context = self._build_context(user_id, username)
        message = f"USER_ACTION: {action}"
        if details:
            message += f" - {details}"
        self.logger.info(f"{context}{message}")
    
    def log_bot_start(self):
        """Log bot startup"""
        self.info("🤖 Bot started successfully")
    
    def log_bot_stop(self):
        """Log bot shutdown"""
        self.info("🛑 Bot stopped")
    
    def log_search_stats(self, query: str, results_count: int, user_id: int, username: str = None):
        """Log search statistics"""
        self.log_user_action("SEARCH_COMPLETED", user_id, username, 
                           f"query='{query}', results={results_count}")
    
    def log_download_stats(self, title: str, user_id: int, username: str = None):
        """Log download statistics"""
        self.log_user_action("DOWNLOAD_COMPLETED", user_id, username, 
                           f"title='{title[:50]}...' if len(title) > 50 else title")
    
    async def cleanup(self):
        """Cleanup resources"""
        for handler in self.logger.handlers:
            if isinstance(handler, TelegramHandler):
                await handler.close_session()

# Enhanced logger factory function
def create_logger(name: str = "NyaaBot", 
                 log_dir: str = "logs",
                 telegram_token: str = None,
                 telegram_channel: str = None) -> BotLogger:
    """Create a logger instance with optional Telegram integration"""
    
    # Get values from environment if not provided
    if not telegram_token:
        telegram_token = os.getenv("LOG_BOT_TOKEN")
    if not telegram_channel:
        telegram_channel = os.getenv("LOG_CHANNEL_ID")
    
    return BotLogger(
        name=name,
        log_dir=log_dir,
        telegram_token=telegram_token,
        telegram_channel=telegram_channel
    )