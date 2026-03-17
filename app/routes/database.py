from fastapi import APIRouter, Form, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from seeder import create_database
from db import list_databases

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.post("/databases")
async def get_databases(
    request: Request,
    host: str = Form("db"),
    port: str = Form("5432"),
    user: str = Form("postgres"),
    password: str = Form("postgres")
):
    try:
        databases = list_databases(host=host, port=port, user=user, password=password)
    except Exception as e:
        return JSONResponse({"databases": [], "error": str(e)})
    return JSONResponse({"databases": databases})


@router.post("/create-db")
async def create_db(
    request: Request,
    dbname: str = Form(...),
    host: str = Form("db"),
    port: str = Form("5432"),
    user: str = Form("postgres"),
    password: str = Form("postgres")
):
    try:
        create_database(dbname, host=host, port=port, user=user, password=password)
        message = f"✅ Database '{dbname}' created successfully"
    except Exception as e:
        message = f"❌ Error: {str(e)}"
    return templates.TemplateResponse(
        "components/result.html",
        {"request": request, "message": message}
    )
