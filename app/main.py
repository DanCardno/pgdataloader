from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from routes import database, seed

app = FastAPI()
templates = Jinja2Templates(directory="templates")

app.include_router(database.router)
app.include_router(seed.router)

@app.get("/")
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
