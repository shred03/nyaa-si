from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import os

class DatabaseManager:
    def __init__(self, connection_string: str = None, database_name: str = "nyaa_bot"):
        self.connection_string = connection_string or os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.database_name = database_name
        self.client = None
        self.db = None
    
    async def connect(self):
        """Initialize database connection"""
        try:
            self.client = AsyncIOMotorClient(self.connection_string)
            self.db = self.client[self.database_name]
            
            # Test connection
            await self.client.admin.command('ping')
            print("✅ Connected to MongoDB successfully")
            
            # Create indexes
            await self._create_indexes()
            
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            print("📤 Disconnected from MongoDB")
    
    async def _create_indexes(self):
        """Create database indexes for better performance"""
        # Users collection indexes
        await self.db.users.create_index("user_id", unique=True)
        await self.db.users.create_index("username")
        await self.db.users.create_index("created_at")
        
        # Search history indexes
        await self.db.search_history.create_index([("user_id", 1), ("created_at", -1)])
        await self.db.search_history.create_index("query")
        
        # Downloads history indexes
        await self.db.downloads.create_index([("user_id", 1), ("created_at", -1)])
    
    # User Management
    async def create_or_update_user(self, user_id: int, username: str = None, 
                                   first_name: str = None, last_name: str = None) -> Dict[str, Any]:
        """Create new user or update existing user info"""
        now = datetime.now(timezone.utc)
        
        user_data = {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "last_seen": now,
            "updated_at": now
        }
        
        # Update or insert user
        result = await self.db.users.update_one(
            {"user_id": user_id},
            {
                "$set": user_data,
                "$setOnInsert": {
                    "created_at": now,
                    "total_searches": 0,
                    "total_downloads": 0,
                    "is_active": True
                }
            },
            upsert=True
        )
        
        return await self.get_user(user_id)
    
    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by user_id"""
        return await self.db.users.find_one({"user_id": user_id})
    
    async def get_user_stats(self, user_id: int) -> Dict[str, Any]:
        """Get user statistics"""
        user = await self.get_user(user_id)
        if not user:
            return {}
        
        # Get recent activity
        recent_searches = await self.db.search_history.count_documents({
            "user_id": user_id,
            "created_at": {"$gte": datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)}
        })
        
        recent_downloads = await self.db.downloads.count_documents({
            "user_id": user_id,
            "created_at": {"$gte": datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)}
        })
        
        return {
            "total_searches": user.get("total_searches", 0),
            "total_downloads": user.get("total_downloads", 0),
            "today_searches": recent_searches,
            "today_downloads": recent_downloads,
            "member_since": user.get("created_at"),
            "last_seen": user.get("last_seen")
        }
    
    # Search History
    async def save_search(self, user_id: int, query: str, results_count: int = 0) -> str:
        """Save search query to history"""
        search_data = {
            "user_id": user_id,
            "query": query,
            "results_count": results_count,
            "created_at": datetime.now(timezone.utc)
        }
        
        result = await self.db.search_history.insert_one(search_data)
        
        # Update user total searches
        await self.db.users.update_one(
            {"user_id": user_id},
            {"$inc": {"total_searches": 1}}
        )
        
        return str(result.inserted_id)
    
    async def get_user_search_history(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get user's recent search history"""
        cursor = self.db.search_history.find(
            {"user_id": user_id}
        ).sort("created_at", -1).limit(limit)
        
        return await cursor.to_list(length=limit)
    
    async def get_popular_searches(self, limit: int = 10, days: int = 7) -> List[Dict[str, Any]]:
        """Get most popular search queries"""
        from_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        from_date = from_date.replace(day=from_date.day - days)
        
        pipeline = [
            {"$match": {"created_at": {"$gte": from_date}}},
            {"$group": {
                "_id": "$query",
                "count": {"$sum": 1},
                "unique_users": {"$addToSet": "$user_id"}
            }},
            {"$project": {
                "query": "$_id",
                "count": 1,
                "unique_users": {"$size": "$unique_users"}
            }},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]
        
        cursor = self.db.search_history.aggregate(pipeline)
        return await cursor.to_list(length=limit)
    
    # Download History
    async def save_download(self, user_id: int, title: str, magnet_link: str, 
                           size: str = None, seeders: str = None) -> str:
        """Save download to history"""
        download_data = {
            "user_id": user_id,
            "title": title,
            "magnet_link": magnet_link,
            "size": size,
            "seeders": seeders,
            "created_at": datetime.now(timezone.utc)
        }
        
        result = await self.db.downloads.insert_one(download_data)
        
        # Update user total downloads
        await self.db.users.update_one(
            {"user_id": user_id},
            {"$inc": {"total_downloads": 1}}
        )
        
        return str(result.inserted_id)
    
    async def get_user_downloads(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Get user's download history"""
        cursor = self.db.downloads.find(
            {"user_id": user_id}
        ).sort("created_at", -1).limit(limit)
        
        return await cursor.to_list(length=limit)
    
    # Analytics
    async def get_bot_stats(self) -> Dict[str, Any]:
        """Get overall bot statistics"""
        total_users = await self.db.users.count_documents({})
        active_users = await self.db.users.count_documents({
            "last_seen": {
                "$gte": datetime.now(timezone.utc).replace(day=datetime.now().day - 7)
            }
        })
        
        total_searches = await self.db.search_history.count_documents({})
        total_downloads = await self.db.downloads.count_documents({})
        
        # Today's activity
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today_searches = await self.db.search_history.count_documents({
            "created_at": {"$gte": today_start}
        })
        today_downloads = await self.db.downloads.count_documents({
            "created_at": {"$gte": today_start}
        })
        
        return {
            "total_users": total_users,
            "active_users_7d": active_users,
            "total_searches": total_searches,
            "total_downloads": total_downloads,
            "today_searches": today_searches,
            "today_downloads": today_downloads
        }