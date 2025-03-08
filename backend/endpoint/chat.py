from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import pyodbc
import os
from dotenv import load_dotenv
from openai import OpenAI 

app = FastAPI()


# Pydantic Model
class ChatRequest(BaseModel):
    user_input: str

@app.post("/chat")
def chat(request: ChatRequest, db=Depends(get_db)):
    conn, cursor = db
    
    try:
        user_query = request.user_input
        sql_query = cd (f"Convert this to SQL: {user_query}")
        cursor.execute(sql_query)
        result = cursor.fetchall()
        conn.commit()
        return {"query": sql_query, "response": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()