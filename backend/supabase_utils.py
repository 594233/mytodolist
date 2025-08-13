import os
from supabase import create_client, Client
from dotenv import load_dotenv
from pathlib import Path
import tempfile

load_dotenv()

class SupabaseStorage:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_ANON_KEY")
        self.bucket = os.getenv("SUPABASE_BUCKET")
        self.client: Client = create_client(self.url, self.key)
    
    async def upload_chunk(self, identifier: str, chunk_index: int, chunk_data: bytes):
        """上传分片到 Supabase"""
        chunk_path = f"chunks/{identifier}/chunk_{chunk_index}"
        try:
            # 上传前先删除同名分片（忽略不存在的情况）
            self.client.storage.from_(self.bucket).remove([chunk_path])
            # 确保 chunk_data 是 bytes
            if not isinstance(chunk_data, bytes):
                chunk_data = bytes(chunk_data)
            response = self.client.storage.from_(self.bucket).upload(
                path=chunk_path,
                file=chunk_data,
                file_options={"content-type": "application/octet-stream"}
            )
            # 检查返回值
            if hasattr(response, "error") and response.error:
                print("Supabase upload error:", response.error)
                raise Exception(response.error)
            return response
        except Exception as e:
            print(f"upload_chunk error: {e}")
            raise
    
    async def list_chunks(self, identifier: str):
        """列出指定标识符的所有分片"""
        prefix = f"chunks/{identifier}/"
        response = self.client.storage.from_(self.bucket).list(prefix)
        return [item['name'] for item in response] if response else []
    
    async def download_chunk(self, chunk_path: str):
        """下载分片数据"""
        return self.client.storage.from_(self.bucket).download(chunk_path)
    
    async def merge_chunks(self, identifier: str, file_name: str, total_chunks: int):
        """合并分片并上传完整文件"""
        # 创建临时文件
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            
            # 按顺序合并所有分片
            for i in range(total_chunks):
                chunk_path = f"chunks/{identifier}/chunk_{i}"
                chunk_data = await self.download_chunk(chunk_path)
                temp_file.write(chunk_data)
            temp_file.flush()

        # 读取文件内容到内存，关闭文件
        with open(temp_path, 'rb') as f:
            file_bytes = f.read()

        # 上传合并后的文件（此时文件已关闭，不会被占用）
        final_path = f"files/{identifier}/{file_name}"
        response = self.client.storage.from_(self.bucket).upload(
            path=final_path,
            file=file_bytes,
            file_options={"content-type": "application/octet-stream"}
        )

        # 清理临时文件
        os.unlink(temp_path)

        return final_path
        
    async def delete_chunks(self, identifier: str):
        """删除所有分片"""
        chunks = await self.list_chunks(identifier)
        for chunk in chunks:
            self.client.storage.from_(self.bucket).remove([chunk])
    
    async def get_file_url(self, file_path: str, expires_in: int = 3600):
        """获取文件下载 URL"""
        return self.client.storage.from_(self.bucket).create_signed_url(file_path, expires_in)

storage = SupabaseStorage()