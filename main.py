from fastapi import FastAPI
from routers import properties

app = FastAPI(title="Real Estate Search API")

# Root
@app.get("/")
def root():
    return {"message": "FastAPI + OpenSearch backend running"}

# Register routers
app.include_router(properties.router)
