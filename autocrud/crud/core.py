from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Sequence
from typing import IO, Literal, TypeVar
import logging

import msgspec
from fastapi import APIRouter, FastAPI
from fastapi.openapi.utils import get_openapi

from autocrud.crud.route_templates.basic import (
    DependencyProvider,
    FullResourceResponse,
    IRouteTemplate,
    RevisionListResponse,
    jsonschema_to_openapi,
)
from autocrud.crud.route_templates.create import CreateRouteTemplate
from autocrud.crud.route_templates.delete import (
    DeleteRouteTemplate,
    RestoreRouteTemplate,
)
from autocrud.crud.route_templates.get import ReadRouteTemplate
from autocrud.crud.route_templates.patch import (
    RFC6902,
    PatchRouteTemplate,
    RFC6902_Add,
    RFC6902_Copy,
    RFC6902_Move,
    RFC6902_Remove,
    RFC6902_Replace,
    RFC6902_Test,
)
from autocrud.crud.route_templates.search import ListRouteTemplate
from autocrud.crud.route_templates.switch import SwitchRevisionRouteTemplate
from autocrud.crud.route_templates.update import UpdateRouteTemplate
from autocrud.permission.rbac import RBACPermissionChecker
from autocrud.permission.simple import AllowAll
from autocrud.resource_manager.basic import (
    Encoding,
    IStorage,
)
from autocrud.resource_manager.core import ResourceManager
from autocrud.resource_manager.dump_format import (
    DumpStreamReader,
    DumpStreamWriter,
    EofRecord,
    HeaderRecord,
    MetaRecord,
    ModelEndRecord,
    ModelStartRecord,
    RevisionRecord,
)
from autocrud.resource_manager.storage_factory import (
    IStorageFactory,
    MemoryStorageFactory,
)
from autocrud.types import (
    IEventHandler,
    IMigration,
    IPermissionChecker,
    IResourceManager,
    IndexableField,
    ResourceMeta,
    RevisionInfo,
)
from autocrud.util.naming import NameConverter

logger = logging.getLogger(__name__)
T = TypeVar("T")


class AutoCRUD:
    """AutoCRUD - Automatic CRUD API Generator for FastAPI

    AutoCRUD is the main class that automatically generates complete CRUD (Create, Read, Update, Delete)
    APIs for your data models. It provides a powerful, flexible, and easy-to-use system for building
    RESTful APIs with built-in version control, soft deletion, and comprehensive querying capabilities.

    Key Features:
    - **Automatic API Generation**: Generates complete CRUD endpoints for any data model
    - **Version Control**: Built-in revision tracking for all resources with full history
    - **Soft Deletion**: Resources are marked as deleted rather than permanently removed
    - **Flexible Storage**: Support for both memory and disk-based storage backends
    - **Model Agnostic**: Works with msgspec Structs, and other data types
    - **Customizable Routes**: Extensible route template system for custom endpoints
    - **Data Migration**: Built-in support for schema evolution and data migration
    - **Comprehensive Querying**: Advanced filtering, sorting, and pagination capabilities

    Basic Usage:
    ```python
    from fastapi import FastAPI
    from autocrud import AutoCRUD

    # Create AutoCRUD instance
    autocrud = AutoCRUD()

    # Add your model
    autocrud.add_model(User)

    # Apply to FastAPI router
    app = FastAPI()
    autocrud.apply(app)
    ```

    This generates the following endpoints for your User model:
    - `POST /users` - Create a new user
    - `GET /users/data` - List all users (data only)
    - `GET /users/meta` - List all users (metadata only)
    - `GET /users/revision-info` - List all users (revision info only)
    - `GET /users/full` - List all users (complete information)
    - `GET /users/{id}/data` - Get specific user data
    - `GET /users/{id}/meta` - Get specific user metadata
    - `GET /users/{id}/revision-info` - Get specific user revision info
    - `GET /users/{id}/full` - Get complete user information
    - `GET /users/{id}/revision-list` - Get user revision history
    - `PUT /users/{id}` - Update user (full replacement)
    - `PATCH /users/{id}` - Partially update user (JSON Patch)
    - `DELETE /users/{id}` - Soft delete user
    - `POST /users/{id}/restore` - Restore deleted user
    - `POST /users/{id}/switch/{revision_id}` - Switch to specific revision

    Advanced Features:
    - **Custom Storage**: Use disk-based storage for persistence
    - **Data Migration**: Handle schema changes with migration support
    - **Custom Naming**: Control URL patterns and resource names
    - **Route Customization**: Add custom endpoints with route templates
    - **Backup/Restore**: Export and import complete datasets

    Args:
        model_naming: Controls how model names are converted to URL paths.
                     Options: "same", "pascal", "camel", "snake", "kebab" (default)
                     or a custom function that takes a type and returns a string.
        route_templates: Custom list of route templates to use instead of defaults.
                        If None, uses the standard CRUD route templates.

    Example with Advanced Features:
    ```python
    from autocrud import AutoCRUD, DiskStorageFactory
    from pathlib import Path

    # Use disk storage for persistence
    storage_factory = DiskStorageFactory(Path("./data"))

    # Custom naming (convert CamelCase to snake_case)
    autocrud = AutoCRUD(model_naming="snake")

    # Add model with custom configuration
    autocrud.add_model(
        User,
        name="people",  # Custom URL path
        storage_factory=storage_factory,
        id_generator=lambda: f"user_{uuid.uuid4()}",  # Custom ID generation
    )
    ```

    Thread Safety:
    The AutoCRUD instance is thread-safe for read operations, but adding models
    should be done during application startup before handling requests.

    Performance:
    - Memory storage: Suitable for development and small datasets
    - Disk storage: Recommended for production with large datasets
    - All operations are optimized for typical CRUD workloads
    - Built-in pagination prevents memory issues with large result sets

    See Also:
    - IStorageFactory: For implementing custom storage backends
    - IRouteTemplate: For creating custom endpoint templates
    - IResourceManager: For advanced programmatic resource management
    """

    def __init__(
        self,
        *,
        model_naming: Literal["same", "pascal", "camel", "snake", "kebab"]
        | Callable[[type], str] = "kebab",
        route_templates: list[IRouteTemplate] | None = None,
        storage_factory: IStorageFactory | None = None,
        admin: str | None = None,
        permission_checker: IPermissionChecker | None = None,
        dependency_provider: DependencyProvider | None = None,
        event_handlers: Sequence[IEventHandler] | None = None,
    ):
        if storage_factory is None:
            self.storage_factory = MemoryStorageFactory()
        else:
            self.storage_factory = storage_factory
        self.resource_managers: OrderedDict[str, IResourceManager] = OrderedDict()
        self.model_names: dict[type[T], str | None] = {}
        self.model_naming = model_naming
        self.route_templates: list[IRouteTemplate] = (
            [
                CreateRouteTemplate(dependency_provider=dependency_provider),
                ListRouteTemplate(dependency_provider=dependency_provider),
                ReadRouteTemplate(dependency_provider=dependency_provider),
                UpdateRouteTemplate(dependency_provider=dependency_provider),
                PatchRouteTemplate(dependency_provider=dependency_provider),
                SwitchRevisionRouteTemplate(dependency_provider=dependency_provider),
                DeleteRouteTemplate(dependency_provider=dependency_provider),
                RestoreRouteTemplate(dependency_provider=dependency_provider),
            ]
            if route_templates is None
            else route_templates
        )
        self.route_templates.sort()
        if permission_checker is None:
            if not admin:
                self.permission_checker = AllowAll()
            else:
                self.permission_checker = RBACPermissionChecker(
                    storage_factory=self.storage_factory,
                    root_user=admin,
                )
        else:
            self.permission_checker = permission_checker

        self.event_handlers = event_handlers

    def get_resource_manager(self, model: type[T] | str) -> IResourceManager[T]:
        if isinstance(model, str):
            return self.resource_managers[model]
        model_name = self.model_names[model]
        if model_name is None:
            raise ValueError(
                f"Model {model.__name__} is registered with multiple names."
            )
        return self.resource_managers[model_name]

    def _resource_name(self, model: type[T]) -> str:
        """Convert model class name to resource name using the configured naming convention.

        This internal method handles the conversion of Python class names to URL-friendly
        resource names based on the model_naming configuration.

        Args:
            model: The model class whose name should be converted.

        Returns:
            The converted resource name string that will be used in URLs.

        Examples:
            With model_naming="kebab":
            - UserProfile -> "user-profile"
            - BlogPost -> "blog-post"

            With model_naming="snake":
            - UserProfile -> "user_profile"
            - BlogPost -> "blog_post"

            With custom function:
            - Can implement any custom naming logic
        """
        if callable(self.model_naming):
            return self.model_naming(model)
        original_name = model.__name__

        # 使用 NameConverter 進行轉換
        return NameConverter(original_name).to(self.model_naming)

    def add_route_template(self, template: IRouteTemplate) -> None:
        """Add a custom route template to extend the API with additional endpoints.

        Route templates define how to generate specific API endpoints for models.
        By adding custom templates, you can extend the default CRUD functionality
        with specialized endpoints for your use cases.

        Args:
            template: A custom route template implementing IRouteTemplate interface.

        Example:
            ```python
            class CustomSearchTemplate(BaseRouteTemplate):
                def apply(self, model_name, resource_manager, router):
                    @router.get(f"/{model_name}/search")
                    async def search_resources(query: str):
                        # Custom search logic
                        pass


            autocrud = AutoCRUD()
            autocrud.add_route_template(CustomSearchTemplate())
            autocrud.add_model(User)
            ```

        Note:
            Templates are sorted by their order property before being applied.
            Add templates before calling add_model() or apply() for best results.
        """
        self.route_templates.append(template)

    def add_model(
        self,
        model: type[T],
        *,
        name: str | None = None,
        id_generator: Callable[[], str] | None = None,
        storage: IStorage | None = None,
        migration: IMigration | None = None,
        indexed_fields: list[tuple[str, type] | IndexableField] | None = None,
        event_handlers: Sequence[IEventHandler] | None = None,
        permission_checker: IPermissionChecker | None = None,
    ) -> None:
        """Add a data model to AutoCRUD and configure its API endpoints.

        This is the main method for registering models with AutoCRUD. Once added,
        the model will have a complete set of CRUD API endpoints generated automatically.

        Args:
            model: The data model class (msgspec Struct, dataclasses, TypedDict).
            name: Custom resource name for URLs. If None, derived from model class name.
            storage_factory: Custom storage backend. If None, uses in-memory storage.
            id_generator: Custom function for generating resource IDs. If None, uses UUID4.
            migration: Migration handler for schema evolution. Used with disk storage.

        Examples:
            Basic usage:
            ```python
            autocrud.add_model(User)  # Creates /users endpoints
            ```

            With custom name:
            ```python
            autocrud.add_model(User, name="people")  # Creates /people endpoints
            ```

            With persistent storage:
            ```python
            storage = DiskStorageFactory("./data")
            autocrud.add_model(User, storage_factory=storage)
            ```

            With custom ID generation:
            ```python
            autocrud.add_model(User, id_generator=lambda: f"user_{int(time.time())}")
            ```

            With migration support:
            ```python
            class UserMigration(IMigration):
                schema_version = "v2"

                def migrate(self, data, old_version):
                    # Handle schema changes
                    return updated_data


            autocrud.add_model(User, migration=UserMigration())
            ```

        Generated Endpoints:
            For a model named "User", this creates:
            - POST /users - Create new user
            - GET /users/data - List users (data only)
            - GET /users/meta - List users (metadata only)
            - GET /users/{id}/data - Get user data
            - GET /users/{id}/full - Get complete user info
            - PUT /users/{id} - Update user
            - DELETE /users/{id} - Soft delete user
            - And many more...

        Raises:
            ValueError: If model is invalid or conflicts with existing models.

        Note:
            Models should be added during application startup before handling requests.
            The order of adding models doesn't affect the generated APIs.
        """
        _indexed_fields = []
        for field in indexed_fields or []:
            if isinstance(field, IndexableField):
                _indexed_fields.append(field)
            elif (
                isinstance(field, tuple)
                and len(field) == 2
                and isinstance(field[0], str)
                and isinstance(field[1], type)
            ):
                field = IndexableField(field_path=field[0], field_type=field[1])
                _indexed_fields.append(field)
            else:
                raise TypeError(
                    "Invalid indexed field, should be IndexableField or tuple[field_name, field_type]",
                )
        model_name = name or self._resource_name(model)
        if model_name in self.resource_managers:
            raise ValueError(f"Model name {model_name} already exists.")
        if model in self.model_names:
            self.model_names[model] = None
            logger.warning(
                f"Model {model.__name__} is already registered with a different name. "
                f"This resource manager will not be accessible by its type.",
            )
        else:
            self.model_names[model] = model_name
        if storage is None:
            storage = self.storage_factory.build(model, model_name, migration=migration)
        resource_manager = ResourceManager(
            model,
            storage=storage,
            id_generator=id_generator,
            migration=migration,
            indexed_fields=_indexed_fields,
            event_handlers=self.event_handlers or event_handlers,
            permission_checker=self.permission_checker or permission_checker,
        )
        self.resource_managers[model_name] = resource_manager

    def openapi(self, app: FastAPI):
        # Handle root_path by setting servers if not already set
        servers = app.servers
        if app.root_path and not servers:
            servers = [{"url": app.root_path}]

        app.openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            summary=app.summary,
            description=app.description,
            terms_of_service=app.terms_of_service,
            contact=app.contact,
            license_info=app.license_info,
            routes=app.routes,
            webhooks=app.webhooks.routes,
            tags=app.openapi_tags,
            servers=servers,
            separate_input_output_schemas=app.separate_input_output_schemas,
        )
        app.openapi_schema["components"]["schemas"] |= jsonschema_to_openapi(
            [
                ResourceMeta,
                RevisionInfo,
                RevisionListResponse,
                *[rm.resource_type for rm in self.resource_managers.values()],
                *[
                    FullResourceResponse[rm.resource_type]
                    for rm in self.resource_managers.values()
                ],
                RFC6902_Add,
                RFC6902_Remove,
                RFC6902_Replace,
                RFC6902_Move,
                RFC6902_Test,
                RFC6902_Copy,
                RFC6902,
            ],
        )[1]

    def apply(self, router: APIRouter) -> APIRouter:
        """Apply all route templates to generate API endpoints on the given router.

        This method generates all the CRUD endpoints for all registered models
        and applies them to the provided FastAPI router. This is typically the
        final step in setting up your AutoCRUD API.

        Args:
            router: FastAPI APIRouter or FastAPI app instance to add routes to.

        Returns:
            The same router instance with all generated routes added.

        Example:
            ```python
            from fastapi import FastAPI
            from autocrud import AutoCRUD

            app = FastAPI()
            autocrud = AutoCRUD()

            # Add your models
            autocrud.add_model(User)
            autocrud.add_model(Post)

            # Generate and apply all routes
            autocrud.apply(app)

            # Or with a sub-router
            api_router = APIRouter(prefix="/api/v1")
            autocrud.apply(api_router)
            app.include_router(api_router)
            ```

        Generated Routes:
            For each model, applies all route templates in order to create
            a comprehensive set of CRUD endpoints. The exact endpoints depend
            on the route templates configured.

        Note:
            - Call this method after adding all models and custom route templates
            - Each route template is applied to each model in the order specified
            - Routes are generated dynamically based on model structure
            - This method is idempotent - calling it multiple times is safe
        """
        for model_name, resource_manager in self.resource_managers.items():
            for route_template in self.route_templates:
                try:
                    route_template.apply(model_name, resource_manager, router)
                except Exception:
                    pass
        return router

    def dump(self, bio: IO[bytes], *, encoding: Encoding | str = Encoding.json) -> None:
        """Export every model's resources as a specstar ``.acbak`` archive.

        The stream is the ``specstar`` v2 backup format (see
        :mod:`autocrud.resource_manager.dump_format`), so the same file can be
        restored here with :meth:`load` **or** imported into a ``specstar``
        deployment with ``SpecStar.load()`` — this is the supported path for
        migrating a 0.4.x installation to ``specstar``.

        Args:
            bio: Binary stream to write to.
            encoding: Encoding of each revision's payload bytes inside the
                archive — ``"json"`` (default) or ``"msgpack"``. Match it to
                the encoding the *importing* side stores data in (specstar
                defaults to JSON). ``data_hash`` is recomputed over the
                emitted bytes, so it stays consistent either way.

        Example:
            ```python
            with open("backup.acbak", "wb") as f:
                autocrud.dump(f)
            ```

        Notes:
            - Every resource is included, soft-deleted ones too, with its
              complete revision history.
            - Revisions are read through the normal store path, so with a
              ``migration=`` configured, older-schema revisions are migrated
              (and rewritten on disk) exactly as a plain ``get()`` would.
              Back the data directory up first.
        """
        writer = DumpStreamWriter(bio)
        writer.write(HeaderRecord())
        for model_name, mgr in self.resource_managers.items():
            writer.write(ModelStartRecord(model_name=model_name))
            for record in mgr.dump(encoding=Encoding(encoding)):
                writer.write(record)
            writer.write(ModelEndRecord(model_name=model_name))
        writer.write(EofRecord())

    def load(self, bio: IO[bytes]) -> None:
        """Import resources from an archive written by :meth:`dump`.

        Every model in the archive must already be registered with
        :meth:`add_model`. Resources with the same ID are overwritten.

        Args:
            bio: Binary stream to read from.

        Raises:
            ValueError: If the stream is not a v2 archive or names a model
                that is not registered.
        """
        reader = DumpStreamReader(bio)
        try:
            first = next(reader, None)
        except (msgspec.MsgspecError, ValueError) as e:
            raise ValueError(f"Not a dump archive: missing header record ({e}).") from e
        if not isinstance(first, HeaderRecord):
            raise ValueError("Not a dump archive: missing header record.")
        if first.version != 2:
            raise ValueError(f"Unsupported dump format version {first.version}.")

        mgr = None
        for record in reader:
            if isinstance(record, ModelStartRecord):
                if record.model_name not in self.resource_managers:
                    raise ValueError(
                        f"Model {record.model_name!r} not found in resource managers "
                        f"(registered: {', '.join(self.resource_managers) or 'none'}).",
                    )
                mgr = self.resource_managers[record.model_name]
            elif isinstance(record, ModelEndRecord):
                mgr = None
            elif isinstance(record, (MetaRecord, RevisionRecord)):
                if mgr is None:
                    raise ValueError(
                        f"{type(record).__name__} outside of a model section.",
                    )
                mgr.load(record)
            elif isinstance(record, EofRecord):
                break
