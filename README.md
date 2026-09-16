# AutoCRUD

🚀 **自動生成 CRUD API 的 Python 庫** - 支持多種數據類型，零配置快速構建 REST API

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-compatible-green.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **0.4.7 — 0.4.x 只剩一個用途：把資料搬到 [specstar](https://pypi.org/project/specstar/)。**
> `AutoCRUD.dump()` 在 0.4.0–0.4.5 一呼叫就 `TypeError`；0.4.6 修好並改成直接輸出 specstar 的 `.acbak` 備份格式；
> 0.4.7 讓它遇到壞掉的 resource / revision 時**跳過並回報**，不再整個中斷，並會記錄進度：
>
> ```python
> import logging; logging.basicConfig(level=logging.INFO)   # 看進度
> with open("backup.acbak", "wb") as f:
>     report = crud.dump(f)      # {model: DumpReport(resources, revisions, bytes, seconds, skipped=[...])}
> # 之後在 specstar 上：spec.load(open("backup.acbak", "rb"))
> ```
>
> 完整步驟見 [Upgrading from autocrud 0.4.x](https://hychou0515.github.io/specstar/guides/upgrade-from-0.4/)。

## ✨ 特色功能

- 🎯 **多數據類型支持**: TypedDict、dataclass、msgspec.Struct
- ⚡ **零配置**: 一行代碼生成完整 CRUD API
- 🔧 **高度可定制**: 靈活的路由模板和命名約定
- 📚 **自動文檔**: 集成 Swagger/OpenAPI 文檔
- 🏎️ **高性能**: 基於 FastAPI 和 msgspec
- 🔒 **類型安全**: 完整的 TypeScript 風格類型檢查

## 🚀 快速開始

### 安裝

```bash
pip install autocrud
# 或使用 uv
uv add autocrud
```

### 5 分鐘創建 API

```python
from msgspec import Struct
from fastapi import FastAPI, APIRouter
from autocrud.crud.core import (
    AutoCRUD, CreateRouteTemplate, ReadRouteTemplate,
    UpdateRouteTemplate, DeleteRouteTemplate, ListRouteTemplate
)

# 定義數據模型
class User(Struct):
    name: str
    email: str
    age: int = 0

# 創建 AutoCRUD 實例
crud = AutoCRUD(model_naming="kebab")

# 添加 CRUD 操作
crud.add_route_template(CreateRouteTemplate())
crud.add_route_template(ReadRouteTemplate())
crud.add_route_template(UpdateRouteTemplate())
crud.add_route_template(DeleteRouteTemplate())
crud.add_route_template(ListRouteTemplate())

# 註冊模型 - 就這麼簡單！
crud.add_model(User)

# 集成到 FastAPI
app = FastAPI(title="User API")
router = APIRouter()
crud.apply(router)
app.include_router(router)

# 運行: uvicorn main:app --reload
```

**🎉 完成！** 現在你有了一個完整的 CRUD API：

- `POST /user` - 創建用戶
- `GET /user/{id}` - 獲取用戶
- `PUT /user/{id}` - 更新用戶
- `DELETE /user/{id}` - 刪除用戶
- `GET /user` - 列出所有用戶

## 📊 多數據類型支持

AutoCRUD 支持 Python 主流數據類型，你可以選擇最適合的：

```python
from typing import TypedDict, Optional
from dataclasses import dataclass
import msgspec

# 1. TypedDict - 輕量級
class Product(TypedDict):
    name: str
    price: float
    in_stock: bool

# 2. msgspec.Struct - 高性能
class User(msgspec.Struct):
    username: str
    email: str
    age: Optional[int] = 0

# 3. dataclass - 原生支持
@dataclass
class Order:
    customer_id: str
    items: list
    total: float = 0.0

# 4. msgspec - 靈活數據
class Event(msgspec.Struct):
    type: str
    data: dict
    timestamp: float

# 一次註冊所有類型
crud.add_model(Product)   # /product
crud.add_model(User)      # /user  
crud.add_model(Order)     # /order
crud.add_model(Event)     # /event
```

## 🎯 實際示例

### 博客 API

```python
from dataclasses import dataclass
from msgspec import Struct
from typing import List, Optional

class Author(Struct):
    name: str
    email: str
    bio: Optional[str] = ""

@dataclass
class BlogPost:
    title: str
    content: str
    author_id: str
    tags: List[str] = None
    published: bool = False

# 創建完整博客 API
crud = AutoCRUD(model_naming="kebab")
# ... 添加路由模板
crud.add_model(Author)    # /author
crud.add_model(BlogPost)  # /blog-post
```

### 電商系統

```python
from decimal import Decimal
from enum import Enum
from msgspec import Struct

class OrderStatus(str, Enum):
    PENDING = "pending"
    SHIPPED = "shipped"
    DELIVERED = "delivered"

class Product(Struct):
    name: str
    price: Decimal
    stock: int
    category: str

class Order(Struct):
    customer_id: str
    items: List[dict]
    status: OrderStatus = OrderStatus.PENDING
    total: Decimal

# 完整電商 CRUD
crud.add_model(Product)  # /product
crud.add_model(Order)    # /order
```

## ⚙️ 配置選項

### 命名約定

```python
# kebab-case (推薦)
crud = AutoCRUD(model_naming="kebab")
# UserProfile -> /user-profile

# snake_case
crud = AutoCRUD(model_naming="snake") 
# UserProfile -> /user_profile

# 自定義
def custom_naming(model_type):
    return f"api_{model_type.__name__.lower()}"
crud = AutoCRUD(model_naming=custom_naming)
```

### 選擇性 CRUD 操作

```python
# 只讀 API
crud.add_route_template(ReadRouteTemplate())
crud.add_route_template(ListRouteTemplate())

# 基本 CRUD
crud.add_route_template(CreateRouteTemplate())
crud.add_route_template(ReadRouteTemplate()) 
crud.add_route_template(UpdateRouteTemplate())
crud.add_route_template(DeleteRouteTemplate())

# 高級功能
crud.add_route_template(PatchRouteTemplate())        # 部分更新
crud.add_route_template(SwitchRevisionRouteTemplate()) # 版本控制
crud.add_route_template(RestoreRouteTemplate())       # 恢復刪除
```

## 📖 文檔

- 📚 [完整文檔](docs/source/index.md)
- 🚀 [快速開始](docs/source/quickstart_new.md)
- 📖 [用戶指南](docs/source/user_guide_new.md)
- 💡 [示例集合](docs/source/examples_new.md)
- 🔧 [API 參考](docs/source/api_reference.md)

## 🏃‍♂️ 運行示例

克隆倉庫並運行示例：

```bash
git clone https://github.com/HYChou0515/autocrud.git
cd autocrud

# 博客 API 示例
python examples/blog_api_example.py

# 電商 API 示例  
python examples/ecommerce_api_example.py

# 多數據類型示例
python examples/simple_quickstart.py
```

然後訪問 http://localhost:8000/docs 查看 API 文檔。

## 🧪 測試

```bash
# 安裝依賴
uv install

# 運行測試
uv run pytest

# 運行特定測試
uv run pytest tests/test_multiple_data_types.py -v
```

## 🤝 貢獻

歡迎貢獻！請查看 [貢獻指南](docs/source/contributing.md)。

## 📄 許可證

MIT License - 詳見 [LICENSE](LICENSE) 文件。

## 🌟 為什麼選擇 AutoCRUD？

### 傳統方式 😫
```python
# 需要手寫大量樣板代碼
@app.post("/users")
async def create_user(user: UserCreate):
    # 驗證邏輯
    # 業務邏輯  
    # 數據庫操作
    # 錯誤處理
    # 響應格式化
    pass

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    # 更多樣板代碼...
    pass

# ... 重複 5+ 個端點
```

### AutoCRUD 方式 😎
```python
# 一行代碼，完整 CRUD API
crud.add_model(User)
```

**節省 90% 的開發時間！** 🎯
