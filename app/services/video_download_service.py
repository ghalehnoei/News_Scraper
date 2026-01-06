"""Service for managing video download tasks."""

import asyncio
import uuid
from datetime import datetime
from typing import Dict, Optional, Callable
from enum import Enum

from app.core.logging import setup_logging
from app.services.reuters_video_downloader import ReutersVideoDownloader

logger = setup_logging()


class DownloadStatus(str, Enum):
    """Download task status."""
    PENDING = "pending"
    AUTHENTICATING = "authenticating"
    FETCHING = "fetching"
    EXTRACTING = "extracting"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    ERROR = "error"


class DownloadTask:
    """Represents a download task."""
    
    def __init__(self, task_id: str, news_id: str, item_id: str):
        self.task_id = task_id
        self.news_id = news_id
        self.item_id = item_id
        self.status = DownloadStatus.PENDING
        self.progress = 0
        self.message = "در حال آماده‌سازی..."
        self.s3_key: Optional[str] = None
        self.presigned_url: Optional[str] = None
        self.error: Optional[str] = None
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self._task: Optional[asyncio.Task] = None
    
    def to_dict(self) -> dict:
        """Convert task to dictionary."""
        return {
            "task_id": self.task_id,
            "news_id": self.news_id,
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "s3_key": self.s3_key,
            "presigned_url": self.presigned_url,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class VideoDownloadService:
    """Service for managing video download tasks."""
    
    def __init__(self):
        """Initialize the service."""
        self._tasks: Dict[str, DownloadTask] = {}
        self._lock = asyncio.Lock()
    
    async def start_download(self, news_id: str, item_id: str) -> str:
        """
        Start a download task.
        
        Args:
            news_id: News article ID
            item_id: Reuters item ID (GUID)
            
        Returns:
            Task ID
        """
        task_id = str(uuid.uuid4())
        
        task = DownloadTask(task_id, news_id, item_id)
        
        async with self._lock:
            self._tasks[task_id] = task
        
        # Start download in background
        download_task = asyncio.create_task(self._download_task(task))
        task._task = download_task
        
        logger.info(f"Started download task {task_id} for news_id {news_id}, item_id {item_id}")
        
        return task_id
    
    async def get_task_status(self, task_id: str) -> Optional[dict]:
        """
        Get task status.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task status dictionary or None if not found
        """
        async with self._lock:
            task = self._tasks.get(task_id)
            if task:
                return task.to_dict()
            return None
    
    async def _download_task(self, task: DownloadTask) -> None:
        """Execute download task."""
        try:
            downloader = ReutersVideoDownloader()
            
            def progress_callback(data: dict):
                """Update task progress."""
                status_str = data.get('status', 'pending')
                # Map status strings to DownloadStatus enum
                status_map = {
                    'authenticating': DownloadStatus.AUTHENTICATING,
                    'fetching': DownloadStatus.FETCHING,
                    'extracting': DownloadStatus.EXTRACTING,
                    'downloading': DownloadStatus.DOWNLOADING,
                    'uploading': DownloadStatus.UPLOADING,
                    'completed': DownloadStatus.COMPLETED,
                    'success': DownloadStatus.COMPLETED,
                    'error': DownloadStatus.ERROR,
                }
                task.status = status_map.get(status_str, DownloadStatus.PENDING)
                task.progress = data.get('progress', 0)
                task.message = data.get('message', '')
                task.updated_at = datetime.now()
                
                if 's3_key' in data:
                    task.s3_key = data['s3_key']
            
            # Start download
            task.status = DownloadStatus.AUTHENTICATING
            task.message = "در حال احراز هویت..."
            task.progress = 0
            
            s3_key = await downloader.download_and_upload(task.item_id, progress_callback)
            
            if s3_key:
                task.status = DownloadStatus.COMPLETED
                task.progress = 100
                task.message = "دانلود با موفقیت انجام شد"
                task.s3_key = s3_key
                
                # Generate presigned URL
                from app.storage.s3 import generate_presigned_url
                task.presigned_url = await generate_presigned_url(s3_key)
                
                logger.info(f"Download task {task.task_id} completed successfully: {s3_key}")
            else:
                task.status = DownloadStatus.ERROR
                task.error = "دانلود با خطا مواجه شد"
                task.message = "خطا در دانلود ویدئو"
                logger.error(f"Download task {task.task_id} failed")
                
        except Exception as e:
            task.status = DownloadStatus.ERROR
            task.error = str(e)
            task.message = f"خطا: {str(e)}"
            task.updated_at = datetime.now()
            logger.error(f"Error in download task {task.task_id}: {e}", exc_info=True)
    
    async def cleanup_old_tasks(self, max_age_hours: int = 24) -> int:
        """
        Clean up old completed/error tasks.
        
        Args:
            max_age_hours: Maximum age in hours for tasks to keep
            
        Returns:
            Number of tasks cleaned up
        """
        from datetime import timedelta
        
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        cleaned = 0
        
        async with self._lock:
            tasks_to_remove = []
            for task_id, task in self._tasks.items():
                if (task.status in [DownloadStatus.COMPLETED, DownloadStatus.ERROR] and 
                    task.updated_at < cutoff_time):
                    tasks_to_remove.append(task_id)
            
            for task_id in tasks_to_remove:
                del self._tasks[task_id]
                cleaned += 1
        
        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} old download tasks")
        
        return cleaned


# Global service instance
_download_service: Optional[VideoDownloadService] = None


def get_download_service() -> VideoDownloadService:
    """Get the global download service instance."""
    global _download_service
    if _download_service is None:
        _download_service = VideoDownloadService()
    return _download_service

