import typing
import logging
from django.db import connection, transaction
from django.db.models import Model, Field


log = logging.getLogger(__name__)
GenModel = typing.TypeVar('GenModel', bound=Model)


def create_temporary_model(model_klass: GenModel) -> type(GenModel):
    model_name: str = model_klass._meta.model_name

    class Meta:
        db_table: str = f"temp_{model_name}"
        managed: bool = False

    attrs: dict[str, str] = {
        "__module__": __name__,
        "Meta": Meta,
    }

    for field in model_klass._meta.get_fields():
        if isinstance(field, Field):
            attrs[field.name] = field.clone()

    # Dynamically made temporary model
    return type(f"Temp{model_name.capitalize()}", (Model,), attrs)


def create_temporary_table(real_model_klass: GenModel, temp_model_klass: type(GenModel)):
    sql_command: str = f"CREATE TEMPORARY TABLE {temp_model_klass._meta.db_table} AS SELECT * FROM {real_model_klass._meta.db_table} LIMIT 0"
    with connection.cursor() as cursor:
        cursor.execute(sql_command)


def move_records_to_temporary_table(instances: list[GenModel], batch_size: int = None):
    """It creates temporary table in DB for session and copy instances there ...

    Args:
        instances: list of original instances to sync
        batch_size: how many records are saved at once with django bulk

    Returns:
        nothing
    """
    if len(instances) == 0:
        # nothing to do
        return
    else:
        try:
            model_klass: GenModel = instances[0].__class__
        except AttributeError:
            log.exception("Cannot get model_klass from list of instances to sync.")
            return

    temp_model_klass: GenModel = create_temporary_model(model_klass=model_klass)
    create_temporary_table(model_klass, temp_model_klass)

    temp_model_klass.objects.bulk_create(instances, batch_size=batch_size)


def bulk_sync(model_klass: GenModel, temp_model_klass: GenModel, key_fields: list[str], fields: list[str]=None, exclude_fields: list[str]=None, skip_creates: bool=True, skip_updates: bool=True, skip_deletes: bool=True):
    stats = {"inserted": 0, "updated": 0, "deleted": 0}
    set_key_fields = set(key_fields)
    set_fields = set(fields)
    set_exclude_fields = set(exclude_fields)
    with transaction.atomic():
        with connection.cursor() as cursor:
            if not skip_creates:
                insert_fields = ", ".join(set_key_fields | set_fields - set_exclude_fields)
                keys = ", ".join(key_fields)
                select_parts = ", ".join([f"origin.{name}" for name in insert_fields])
                keys_filter = " AND ".join([f"origin.{key_name} = upstream.{key_name}" for key_name in key_fields])
                insert_sql = f"""
    WITH inserted_rows AS (
        INSERT INTO {model_klass._meta.db_table} ({insert_fields})
        SELECT {select_parts} FROM {temp_model_klass._meta.db_table} AS origin
        WHERE NOT EXISTS (
            SELECT 1 FROM {model_klass._meta.db_table} AS upstream WHERE {keys_filter}
        )
        RETURNING {keys}
    )
    DELETE FROM {temp_model_klass._meta.db_table} WHERE {keys} IN (SELECT {keys} FROM inserted_rows);
    SELECT COUNT(*) FROM inserted_rows;"""
                cursor.execute(insert_sql)
                stats["inserted"] = cursor.fetchone()[0]
    return {"stats": stats}
