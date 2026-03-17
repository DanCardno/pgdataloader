from fastapi import APIRouter, Form, Request
from fastapi.templating import Jinja2Templates
from seeder import load_data, order_data

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.post("/seed-data")
async def seed_data(
    request: Request,
    dbname: str = Form(...),
    host: str = Form("db"),
    port: str = Form("5432"),
    user: str = Form("postgres"),
    password: str = Form("postgres")
):
    try:
        load_data(dbname, host=host, port=port, user=user, password=password)
        message = f"✅ Agents & Customers seeded into '{dbname}' successfully"
    except Exception as e:
        message = f"❌ Error: {str(e)}"
    return templates.TemplateResponse(
        "components/result.html",
        {"request": request, "message": message}
    )


@router.post("/seed-orders")
async def seed_orders(
    request: Request,
    dbname: str = Form(...),
    records: int = Form(...),
    host: str = Form("db"),
    port: str = Form("5432"),
    user: str = Form("postgres"),
    password: str = Form("postgres")
):
    try:
        order_data(dbname, records, host=host, port=port, user=user, password=password)
        message = f"✅ {records} orders & calls seeded into '{dbname}' successfully"
    except Exception as e:
        message = f"❌ Error: {str(e)}"
    return templates.TemplateResponse(
        "components/result.html",
        {"request": request, "message": message}
    )
