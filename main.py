from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from generated.query import schema
import uvicorn

graphql_app = GraphQLRouter(schema)

app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)