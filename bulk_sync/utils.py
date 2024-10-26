import typing
import logging
from django.db import connection
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
