import os
import ydb
from dotenv import load_dotenv

load_dotenv()

driver = ydb.Driver(
    ydb.DriverConfig(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.AnonymousCredentials(),
    )
)

driver.wait(fail_fast=True, timeout=5)

pool = ydb.QuerySessionPool(driver)