import os
import base64
import re
import stat
from email.utils import formatdate
from mimetypes import guess_type
from pathlib import Path
from urllib.parse import quote
import uuid

import aiofiles
from fastapi import FastAPI, Body, File, UploadFile, Request, HTTPException
from fastapi.responses import JSONResponse
from supabase_utils import storage
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(docs_url="/docs")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 或指定你的前端地址
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.post("/file-slice")
async def upload_file(
    request: Request,
    identifier: str = Body(..., description="文件唯一标识符"),
    number: int = Body(..., description="文件分片序号（从0开始）"),
    file: UploadFile = File(..., description="文件分片")
):
    """文件分片上传到 Supabase"""

    try:
        # 读取分片数据
        chunk_data = await file.read()
        
        # 上传到 Supabase
        await storage.upload_chunk(identifier, number, chunk_data)
        
        return JSONResponse({
            'code': 1,
            'chunk': f'{identifier}_{number}',
            'message': '分片上传成功'
        })
    
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"分片上传失败: {str(e)}"
        )

@app.put("/file-slice")
async def merge_file(
    request: Request,
    name: str = Body(..., description="文件名称（不含后缀）"),
    file_type: str = Body(..., description="文件类型/后缀"),
    identifier: str = Body(..., description="文件唯一标识符"),
    total_chunks: int = Body(..., description="总分片数量")
):
    """合并分片文件并上传到 Supabase"""
    print("merge_file params:", name, file_type, identifier, total_chunks)

    try:
        # 验证分片完整性
        chunks = await storage.list_chunks(identifier)
        if len(chunks) < total_chunks:
            missing = total_chunks - len(chunks)
            raise HTTPException(
                status_code=400,
                detail=f"缺少分片: 需要 {total_chunks} 个分片，但只找到 {len(chunks)} 个，缺失 {missing} 个"
            )
        
        # 合并分片
        file_name = f"{name}.{file_type}"
        file_path = await storage.merge_chunks(identifier, file_name, total_chunks)
        
        # 清理分片
        await storage.delete_chunks(identifier)
        
        # 获取下载 URL
        download_url = await storage.get_file_url(file_path)
        
        return JSONResponse({
            'code': 1,
            'file_path': file_path,
            'download_url': download_url,
            'file_name': file_name
        })
    
    except Exception as e:
        print("merge_file error:", e)
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500, 
            detail=f"文件合并失败: {str(e)}"
        )

@app.get("/upload-status/{identifier}")
async def check_upload_status(
    identifier: str,
    total_chunks: int = None
):
    """检查上传状态"""
    try:
        chunks = await storage.list_chunks(identifier)
        uploaded_chunks = [int(chunk.split("_")[-1]) for chunk in chunks]
        
        response = {
            'code': 1,
            'identifier': identifier,
            'uploaded_chunks': uploaded_chunks,
            'total_uploaded': len(uploaded_chunks)
        }
        
        if total_chunks:
            response['progress'] = round(len(uploaded_chunks) / total_chunks * 100, 2)
            response['missing_chunks'] = [
                i for i in range(total_chunks) 
                if i not in uploaded_chunks
            ]
        
        return JSONResponse(response)
    
    except Exception as e:

        raise HTTPException(
            status_code=500, 
            
            detail=f"获取上传状态失败: {str(e)}"
        )

@app.get("/file-slice/{file_path:path}")
async def download_file(file_path: str):
    """获取文件下载 URL"""
    try:
        # 验证文件路径格式
        if not file_path.startswith("files/"):
            raise HTTPException(
                status_code=400,
                detail="无效的文件路径格式"
            )
        
        # 获取下载 URL
        download_url = await storage.get_file_url(file_path)
        
        return JSONResponse({
            'code': 1,
            'download_url': download_url
        })
    
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"获取下载链接失败: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app=app, host="127.0.0.1", port=8000)